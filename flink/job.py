# flink/job.py — Global stream feature builder + HTTP inference caller
# Reads raw machine events from Kafka, builds online features, calls service /infer,
# and writes predictions to Kafka.

import os
import re
import json
import time
import math
import pickle
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from pyflink.common import Types, Duration, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.environ.get("TOPIC_IN", "iot.machine.status.raw")
TOPIC_OUT = os.environ.get("TOPIC_OUT", "ml.pred.alert.eta")
TOPIC_DLQ = os.environ.get("TOPIC_DLQ", "iot.machine.status.dlq")

SERVICE_URL = os.environ.get("SERVICE_URL", "http://172.18.0.2:30080/infer")
SERVICE_METADATA_URL = os.environ.get("SERVICE_METADATA_URL", SERVICE_URL.replace("/infer", "/metadata"))

WATERMARK_LATENESS_MIN = int(os.environ.get("WATERMARK_LATENESS_MIN", "3"))
VERY_LATE_MAX_AGE_MIN = int(os.environ.get("VERY_LATE_MAX_AGE_MIN", "120"))
HTTP_TIMEOUT_SEC = float(os.environ.get("HTTP_TIMEOUT_SEC", "2.0"))

DEFAULT_PROCESS = os.environ.get("DEFAULT_PROCESS", "unknown")
DEFAULT_PLANT = os.environ.get("DEFAULT_PLANT", "")

# Fallback only if service metadata is unavailable at job startup.
FALLBACK_NORMAL_STATUSES = {s.strip() for s in os.environ.get("NORMAL_STATUSES", "run").split(",") if s.strip()}
FALLBACK_TARGET_STATUSES = {s.strip() for s in os.environ.get("TARGET_STATUSES", "").split(",") if s.strip()}
FALLBACK_ROLLING_WINDOWS = [int(x) for x in os.environ.get("ROLLING_WINDOWS_MIN", "5,15,30,60,120").split(",") if x.strip()]


# ---------------------------------------------------------------------
# Text/status helpers
# ---------------------------------------------------------------------
def normalize_status_text(x) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", "_", s)
    s = s.replace("/", "_")
    s = s.replace("-", "_")
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def sanitize_feature_token(x) -> str:
    s = normalize_status_text(x)
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "missing"


def canonicalize_status(x, alias_map):
    s = normalize_status_text(x)
    alias_norm = {normalize_status_text(k): normalize_status_text(v) for k, v in (alias_map or {}).items()}
    return alias_norm.get(s, s)


def parse_iso_utc(s: str) -> datetime:
    return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)


def seconds_between(a: datetime, b: datetime):
    if a is None or b is None:
        return None
    return (a - b).total_seconds()


def fetch_metadata():
    try:
        r = requests.get(SERVICE_METADATA_URL, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        meta = r.json()
        return {
            "feature_cols": meta.get("feature_cols", []),
            "rolling_windows_min": meta.get("rolling_windows_min", FALLBACK_ROLLING_WINDOWS),
            "normal_statuses": set(normalize_status_text(s) for s in meta.get("normal_statuses", list(FALLBACK_NORMAL_STATUSES))),
            "target_statuses": set(normalize_status_text(s) for s in meta.get("target_statuses", list(FALLBACK_TARGET_STATUSES))),
            "all_statuses": [normalize_status_text(s) for s in meta.get("all_statuses", [])],
            "status_alias_map": meta.get("status_alias_map", {}) or {},
            "artifact_schema": meta.get("artifact_schema", "unknown"),
        }
    except Exception as e:
        print(f"WARNING: cannot fetch service metadata from {SERVICE_METADATA_URL}: {e}")
        return {
            "feature_cols": [],
            "rolling_windows_min": FALLBACK_ROLLING_WINDOWS,
            "normal_statuses": set(normalize_status_text(s) for s in FALLBACK_NORMAL_STATUSES),
            "target_statuses": set(normalize_status_text(s) for s in FALLBACK_TARGET_STATUSES),
            "all_statuses": [],
            "status_alias_map": {},
            "artifact_schema": "fallback",
        }


# ---------------------------------------------------------------------
# State
# ---------------------------------------------------------------------
class FeatureState:
    def __init__(self):
        self.first_seen_ts = None
        self.last_event_ts = None
        self.event_index = 0
        self.event_gaps = []  # last gaps, newest last

        self.prev_status_tokens = []  # newest first
        self.last_status_token = None
        self.status_segment_start_ts = None
        self.consecutive_same_status_count = 0

        self.last_target_ts = None
        self.last_target_type_token = "none"
        self.target_event_count_prior = 0

        self.last_time_by_status = {}

        self.events = []   # timestamps, for rolling windows
        self.targets = []
        self.status_events = defaultdict(list)

    def prune(self, ts: datetime, max_window_min: int):
        cutoff = ts - timedelta(minutes=max_window_min)
        self.events = [t for t in self.events if t >= cutoff]
        self.targets = [t for t in self.targets if t >= cutoff]
        for k in list(self.status_events.keys()):
            self.status_events[k] = [t for t in self.status_events[k] if t >= cutoff]


# ---------------------------------------------------------------------
# FeatureBuilder
# ---------------------------------------------------------------------
class FeatureBuilder(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        desc = ValueStateDescriptor("global_feature_state", Types.PICKLED_BYTE_ARRAY())
        self.state = runtime_context.get_state(desc)
        self.meta = fetch_metadata()
        self.feature_cols = set(self.meta.get("feature_cols", []))
        self.rolling_windows = [int(x) for x in self.meta.get("rolling_windows_min", FALLBACK_ROLLING_WINDOWS)]
        self.max_window_min = max(self.rolling_windows) if self.rolling_windows else 120
        self.normal_statuses = self.meta.get("normal_statuses", set())
        self.target_statuses = self.meta.get("target_statuses", set())
        self.alias_map = self.meta.get("status_alias_map", {})
        self.all_statuses = set(self.meta.get("all_statuses", []))
        if not self.all_statuses:
            self.all_statuses = set(self.normal_statuses) | set(self.target_statuses)

        print("FeatureBuilder metadata:", json.dumps({
            "artifact_schema": self.meta.get("artifact_schema"),
            "feature_count": len(self.feature_cols),
            "normal_statuses": sorted(self.normal_statuses),
            "target_statuses": sorted(self.target_statuses),
            "rolling_windows": self.rolling_windows,
        }))

    def _is_target(self, status: str) -> bool:
        if self.target_statuses:
            return status in self.target_statuses
        return status not in self.normal_statuses

    def _rolling_count(self, timestamps, ts, minutes):
        cutoff = ts - timedelta(minutes=int(minutes))
        return float(sum(1 for t in timestamps if t >= cutoff and t <= ts))

    def _maybe_set_ohe(self, feats, prefix, token, value=1.0):
        col = f"{prefix}_{sanitize_feature_token(token)}"
        feats[col] = float(value)

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            mc_no = str(value.get("mc_no", "")).strip()
            process = str(value.get("process") or DEFAULT_PROCESS).strip().lower()
            plant = str(value.get("plant") or DEFAULT_PLANT).strip().lower() or None
            raw_status = value.get("mc_status", "")
            status = canonicalize_status(raw_status, self.alias_map)
            status_token = sanitize_feature_token(status)
            occurred_ts = parse_iso_utc(value.get("occurred_ts"))

            if not mc_no or not process or not status:
                raise ValueError(f"Missing required event fields: {value}")

            current_wm = ctx.timer_service().current_watermark()
            if current_wm > 0:
                wm_dt = datetime.fromtimestamp(current_wm / 1000.0, tz=timezone.utc)
                if occurred_ts < (wm_dt - timedelta(minutes=VERY_LATE_MAX_AGE_MIN)):
                    dlq = {
                        "dead_event": value,
                        "reason": "very_late",
                        "received_at": datetime.now(timezone.utc).isoformat(),
                        "original_topic": TOPIC_IN,
                        "schema_version": 1,
                    }
                    yield ("DLQ", json.dumps(dlq))
                    return

            raw_state = self.state.value()
            st = FeatureState() if raw_state is None else pickle.loads(raw_state)

            is_target = self._is_target(status)

            # ----------------------------
            # Feature construction BEFORE updating target history for current row
            # ----------------------------
            feats = {}

            # Base time features — new schema
            hour = occurred_ts.hour
            minute = occurred_ts.minute
            second = occurred_ts.second
            weekday = occurred_ts.weekday()
            day = occurred_ts.day
            feats.update({
                "hour": hour,
                "minute": minute,
                "second": second,
                "weekday": weekday,
                "day": day,
                "is_weekend": 1.0 if weekday >= 5 else 0.0,
                "is_night": 1.0 if hour in [0, 1, 2, 3, 4, 5] else 0.0,
                "hour_sin": math.sin(2 * math.pi * hour / 24.0),
                "hour_cos": math.cos(2 * math.pi * hour / 24.0),
                "minute_sin": math.sin(2 * math.pi * minute / 60.0),
                "minute_cos": math.cos(2 * math.pi * minute / 60.0),
                "weekday_sin": math.sin(2 * math.pi * weekday / 7.0),
                "weekday_cos": math.cos(2 * math.pi * weekday / 7.0),
            })

            # Legacy aliases for old 39-feature artifacts
            feats["min_sin"] = feats["minute_sin"]
            feats["min_cos"] = feats["minute_cos"]
            feats["wday_sin"] = feats["weekday_sin"]
            feats["wday_cos"] = feats["weekday_cos"]

            # Sequence and event gap
            if st.first_seen_ts is None:
                st.first_seen_ts = occurred_ts
            event_gap = seconds_between(occurred_ts, st.last_event_ts)
            feats["machine_event_index"] = float(st.event_index)
            feats["time_since_first_seen_machine_sec"] = seconds_between(occurred_ts, st.first_seen_ts) or 0.0
            feats["event_gap_sec"] = event_gap

            # Legacy naming
            feats["time_since_status_change"] = 0.0
            feats["consecutive_same_status"] = 1.0

            # Gap lags and rolling last-5 stats
            gaps = [g for g in st.event_gaps if g is not None]
            for lag in [1, 2, 3]:
                feats[f"event_gap_lag{lag}_sec"] = gaps[-lag] if len(gaps) >= lag else None
            last5 = gaps[-4:] + ([event_gap] if event_gap is not None else [])
            if last5:
                feats["event_gap_mean_5_sec"] = float(sum(last5) / len(last5))
                feats["event_gap_std_5_sec"] = float(np_std(last5))
                feats["event_gap_median_5_sec"] = float(np_median(last5))
                feats["event_gap_min_5_sec"] = float(min(last5))
                feats["event_gap_max_5_sec"] = float(max(last5))

            # Legacy inter-alert gap features
            feats["inter_alert"] = event_gap if is_target else None
            for lag in [1, 2, 3]:
                feats[f"inter_alert_lag{lag}"] = gaps[-lag] if len(gaps) >= lag else None

            # Previous status one-hots
            self._maybe_set_ohe(feats, "status", status_token, 1.0)
            for lag in [1, 2, 3]:
                prev = st.prev_status_tokens[lag - 1] if len(st.prev_status_tokens) >= lag else "none"
                self._maybe_set_ohe(feats, f"prev{lag}_status", prev, 1.0)

            # Last target type one-hot before current row
            self._maybe_set_ohe(feats, "last_target", st.last_target_type_token or "none", 1.0)
            # Legacy naming
            self._maybe_set_ohe(feats, "last_alert", st.last_target_type_token or "none", 1.0)

            # Target history before current row
            feats["time_since_last_target_sec"] = seconds_between(occurred_ts, st.last_target_ts)
            feats["time_since_last_alert"] = feats["time_since_last_target_sec"] if feats["time_since_last_target_sec"] is not None else -1.0
            feats["target_event_count_prior"] = float(st.target_event_count_prior)

            # Status segment
            status_changed = 1.0 if st.last_status_token is None or status_token != st.last_status_token else 0.0
            if status_changed:
                segment_start = occurred_ts
                consecutive = 0.0
            else:
                segment_start = st.status_segment_start_ts or occurred_ts
                consecutive = float(st.consecutive_same_status_count)
            feats["status_changed"] = status_changed
            feats["time_since_status_change_sec"] = seconds_between(occurred_ts, segment_start) or 0.0
            feats["time_since_status_change"] = feats["time_since_status_change_sec"]
            feats["consecutive_same_status_count"] = consecutive
            feats["consecutive_same_status"] = consecutive + 1.0

            # Time since each known status
            for s in self.all_statuses:
                token = sanitize_feature_token(s)
                last_ts = st.last_time_by_status.get(token)
                feats[f"time_since_status_{token}_sec"] = seconds_between(occurred_ts, last_ts)

            # Rolling windows include current event/status/target
            st.events.append(occurred_ts)
            st.status_events[status_token].append(occurred_ts)
            if is_target:
                st.targets.append(occurred_ts)

            for w in self.rolling_windows:
                events_count = self._rolling_count(st.events, occurred_ts, w)
                targets_count = self._rolling_count(st.targets, occurred_ts, w)
                feats[f"events_{w}m_count"] = events_count
                feats[f"events_{w}m_rate_per_min"] = events_count / max(w, 1)
                feats[f"targets_{w}m_count"] = targets_count
                feats[f"targets_{w}m_rate_per_min"] = targets_count / max(w, 1)

                # legacy equivalent for 15m/60m
                if w in [15, 60]:
                    feats[f"events_{w}m"] = events_count
                    feats[f"alerts_{w}m"] = targets_count
                    feats[f"alert_rate_{w}m"] = targets_count / max(events_count, 1.0)

                for s in self.all_statuses:
                    token = sanitize_feature_token(s)
                    c = self._rolling_count(st.status_events.get(token, []), occurred_ts, w)
                    feats[f"status_{token}_{w}m_count"] = c
                    feats[f"status_{token}_{w}m_rate_per_min"] = c / max(w, 1)

            # Update state after building current-row features
            if event_gap is not None:
                st.event_gaps.append(float(event_gap))
                st.event_gaps = st.event_gaps[-20:]
            st.last_event_ts = occurred_ts
            st.event_index += 1

            if status_changed:
                st.status_segment_start_ts = occurred_ts
                st.consecutive_same_status_count = 0
            else:
                st.consecutive_same_status_count += 1
            st.last_status_token = status_token
            st.prev_status_tokens = [status_token] + st.prev_status_tokens[:5]
            st.last_time_by_status[status_token] = occurred_ts

            if is_target:
                st.last_target_ts = occurred_ts
                st.last_target_type_token = status_token
                st.target_event_count_prior += 1

            st.prune(occurred_ts, self.max_window_min)
            self.state.update(pickle.dumps(st))

            out = {
                "event_id": value.get("event_id"),
                "plant": plant,
                "process": process,
                "mc_no": mc_no,
                "occurred_ts": value.get("occurred_ts"),
                "mc_status": status,
                "features": feats,
            }
            yield ("OK", json.dumps(out))

        except Exception as e:
            dlq = {
                "dead_event": value,
                "reason": f"feature_exception:{e}",
                "received_at": datetime.now(timezone.utc).isoformat(),
                "original_topic": TOPIC_IN,
                "schema_version": 1,
            }
            yield ("DLQ", json.dumps(dlq))


# Lightweight numpy fallback functions to avoid importing numpy in PyFlink closure problems.
def np_median(values):
    values = sorted([float(x) for x in values])
    n = len(values)
    if n == 0:
        return None
    mid = n // 2
    return values[mid] if n % 2 else (values[mid - 1] + values[mid]) / 2.0


def np_std(values):
    values = [float(x) for x in values]
    if len(values) < 2:
        return 0.0
    m = sum(values) / len(values)
    return math.sqrt(sum((x - m) ** 2 for x in values) / (len(values) - 1))


# ---------------------------------------------------------------------
# HTTP service caller
# ---------------------------------------------------------------------
class HttpInferMap(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        self.session = requests.Session()
        self.timeout = HTTP_TIMEOUT_SEC

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def map(self, value):
        tag, payload = value
        if tag == "DLQ":
            return ("DLQ", payload)

        try:
            data = json.loads(payload)
            req = {
                "plant": data.get("plant"),
                "process": data.get("process"),
                "mc_no": data["mc_no"],
                "mc_status": data.get("mc_status"),
                "occurred_ts": data["occurred_ts"],
                "features": data.get("features", {}),
            }
            resp = self.session.post(SERVICE_URL, json=req, timeout=self.timeout)
            if resp.status_code != 200:
                dlq = {
                    "dead_event": data,
                    "reason": f"service_{resp.status_code}:{resp.text[:300]}",
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "original_topic": TOPIC_IN,
                    "schema_version": 1,
                }
                return ("DLQ", json.dumps(dlq))

            pred = resp.json()
            pred_rec = {
                "pred_id": f"{data.get('event_id','')}-{int(time.time()*1000)}",
                "source_event_id": data.get("event_id"),
                "plant": data.get("plant"),
                "process": data.get("process"),
                "mc_no": data["mc_no"],
                "mc_status": data.get("mc_status"),
                "now_ts": datetime.now(timezone.utc).isoformat(),
                **pred,
            }
            return ("OUT", json.dumps(pred_rec))

        except Exception as e:
            dlq = {
                "dead_event": payload,
                "reason": f"http_exception:{e}",
                "received_at": datetime.now(timezone.utc).isoformat(),
                "original_topic": TOPIC_IN,
                "schema_version": 1,
            }
            return ("DLQ", json.dumps(dlq))


# ---------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "8")))

    jar_path = os.environ.get("FLINK_KAFKA_JAR", "")
    if jar_path:
        if not jar_path.startswith("file://"):
            jar_path = "file://" + os.path.abspath(jar_path)
        env.add_jars(jar_path)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_topics(TOPIC_IN) \
        .set_group_id(os.environ.get("FLINK_GROUP_ID", "flink-alert-eta-global")) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    def ts_assigner(e, ts):
        try:
            obj = json.loads(e)
            return int(parse_iso_utc(obj["occurred_ts"]).timestamp() * 1000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    wm = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_minutes(WATERMARK_LATENESS_MIN)) \
        .with_timestamp_assigner(ts_assigner)

    ds = env.from_source(source, wm, "kafka-source")
    parsed = ds.map(lambda s: json.loads(s))

    # Global key: process + machine. This prevents machine state from mixing across processes.
    keyed = parsed.key_by(lambda d: f"{str(d.get('process') or DEFAULT_PROCESS).strip().lower()}||{str(d.get('mc_no','')).strip()}")

    fb = keyed.process(
        FeatureBuilder(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    mapped = fb.map(
        HttpInferMap(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    outs = mapped.filter(lambda t: t[0] == "OUT").map(lambda t: t[1], output_type=Types.STRING())
    dlqs = mapped.filter(lambda t: t[0] == "DLQ").map(lambda t: t[1], output_type=Types.STRING())

    sink_out = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_OUT)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    sink_dlq = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_DLQ)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    outs.sink_to(sink_out)
    dlqs.sink_to(sink_dlq)

    env.execute("global-alert-eta-stream")


if __name__ == "__main__":
    main()
