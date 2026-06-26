import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ml.pred.alert.eta")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "pred-logger")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/data/predictions"))
FLUSH_EVERY = int(os.environ.get("FLUSH_EVERY", "1000"))
FLUSH_SECS = float(os.environ.get("FLUSH_SECS", "60"))
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COLS = [
    "pred_id",
    "source_event_id",
    "plant",
    "process",
    "mc_no",
    "mc_status",
    "eta_p50_sec",
    "eta_p90_sec",
    "eta_p50_ts",
    "eta_p90_ts",
    "next_type",
    "type_conf",
    "model_version",
    "feature_version",
    "now_ts",
    "logged_at",
]

rows = []
total = 0
last_flush = time.time()


def parse_json(raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def output_path():
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUTPUT_DIR / f"predictions_{day}.parquet"


def flush(reason="manual"):
    global rows, total, last_flush

    if not rows:
        last_flush = time.time()
        return

    path = output_path()
    new_df = pd.DataFrame(rows, columns=COLS)

    if path.exists():
        old_df = pd.read_parquet(path)
        out_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        out_df = new_df

    out_df.to_parquet(path, index=False)

    total += len(rows)
    print(
        f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
        f"Flushed {len(rows)} rows → {path} "
        f"(session total: {total}, file rows: {len(out_df)}, reason: {reason})",
        flush=True,
    )

    rows = []
    last_flush = time.time()


print("=" * 60, flush=True)
print("  GLOBAL PREDICTION LOGGER", flush=True)
print(f"  Topic: {KAFKA_TOPIC}", flush=True)
print(f"  Output: {OUTPUT_DIR}", flush=True)
print(f"  Flush: every {FLUSH_EVERY} rows or {FLUSH_SECS}s, including idle flush", flush=True)
print("=" * 60, flush=True)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=True,
    value_deserializer=parse_json,
)

try:
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=500)

        got_any = False

        for batch in records.values():
            for msg in batch:
                got_any = True
                p = msg.value or {}
                logged_at = datetime.now(timezone.utc).isoformat()

                rows.append([
                    p.get("pred_id"),
                    p.get("source_event_id"),
                    p.get("plant"),
                    p.get("process"),
                    p.get("mc_no"),
                    p.get("mc_status"),
                    p.get("eta_p50_sec"),
                    p.get("eta_p90_sec"),
                    p.get("eta_p50_ts"),
                    p.get("eta_p90_ts"),
                    p.get("next_type"),
                    p.get("type_conf"),
                    p.get("model_version"),
                    p.get("feature_version"),
                    p.get("now_ts"),
                    logged_at,
                ])

                if len(rows) >= FLUSH_EVERY:
                    flush("row_limit")

        if rows and (time.time() - last_flush >= FLUSH_SECS):
            flush("idle_time" if not got_any else "time")

except KeyboardInterrupt:
    flush("shutdown")
finally:
    flush("final")
    consumer.close()
