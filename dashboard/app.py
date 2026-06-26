# dashboard/app.py — Global Streamlit dashboard for Phase 5 predictions

import os
import json
import time
import hashlib
from collections import deque
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import altair as alt
from kafka import KafkaConsumer

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "ml.pred.alert.eta")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "streamlit-eta")
BUFFER_HOURS = int(os.environ.get("BUFFER_HOURS", "24"))
PLOT_MAX_ROWS = int(os.environ.get("PLOT_MAX_ROWS", "2000"))

DEFAULT_CONF_THRESH = float(os.environ.get("TYPE_CONF_THRESHOLD", "0.6"))
DEFAULT_HORIZON_MIN = int(os.environ.get("HORIZON_MIN", "60"))
DEFAULT_REFRESH_SEC = int(os.environ.get("REFRESH_SEC", "10"))

# Comma-separated hidden statuses. Dashboard no longer hardcodes ASSY only.
HIDE_STATUSES = {s.strip().lower().replace(" ", "_") for s in os.environ.get("HIDE_STATUSES", "run,mc_run,no_work,no work").split(",") if s.strip()}

DEFAULT_COLORS = [
    "#e74c3c", "#f39c12", "#8e44ad", "#3498db", "#27ae60",
    "#d35400", "#16a085", "#2c3e50", "#c0392b", "#7f8c8d",
]
UNCERTAIN_COLOR = "#bdc3c7"

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def norm_status(x):
    if x is None:
        return None
    return str(x).strip().lower().replace(" ", "_").replace("/", "_")


def type_color(t: str) -> str:
    if t is None or str(t).upper() == "UNCERTAIN":
        return UNCERTAIN_COLOR
    h = int(hashlib.md5(str(t).encode("utf-8")).hexdigest(), 16)
    return DEFAULT_COLORS[h % len(DEFAULT_COLORS)]


def parse_ts(ts_str: str) -> pd.Timestamp:
    return pd.to_datetime(ts_str, utc=True, errors="coerce")


def get_local_tz():
    return datetime.now().astimezone().tzinfo


def to_display_tz(ts: pd.Series, use_local: bool) -> pd.Series:
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")
    return ts.dt.tz_convert(get_local_tz() if use_local else "UTC")


def make_consumer(bootstrap, topic, group_id, offset_mode: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=None,             # dashboard reader should not commit offsets
        enable_auto_commit=False,
        auto_offset_reset=offset_mode,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: (k.decode("utf-8") if k is not None else None),
        consumer_timeout_ms=1000,
    )


@st.cache_resource(show_spinner=False)
def get_consumer(bootstrap, topic, _group_id, offset_mode):
    return make_consumer(bootstrap, topic, _group_id, offset_mode)


def is_actionable(next_type) -> bool:
    if next_type is None:
        return True
    return norm_status(next_type) not in HIDE_STATUSES


def ingest_from_consumer(consumer, buf_deque, seen_ids, max_age_hours=24):
    msgs = consumer.poll(timeout_ms=300, max_records=1000)
    added = 0
    for _tp, records in msgs.items():
        for rec in records:
            try:
                val = rec.value
                pred_id = val.get("pred_id")
                if pred_id and pred_id in seen_ids:
                    continue

                p50_ts = val.get("eta_p50_ts")
                p90_ts = val.get("eta_p90_ts")
                mc_no = val.get("mc_no")
                nxt = val.get("next_type")
                conf = val.get("type_conf")
                now_ts = val.get("now_ts")
                mc_status = val.get("mc_status")
                process = val.get("process")
                plant = val.get("plant")
                model_version = val.get("model_version")
                feature_version = val.get("feature_version")

                if not (p50_ts and p90_ts and mc_no):
                    continue

                if nxt is not None and not is_actionable(nxt):
                    continue

                row = {
                    "pred_id": pred_id or f"{mc_no}-{rec.offset}",
                    "plant": plant,
                    "process": process,
                    "mc_no": mc_no,
                    "eta_p50_ts": p50_ts,
                    "eta_p90_ts": p90_ts,
                    "eta_p50_sec": val.get("eta_p50_sec"),
                    "eta_p90_sec": val.get("eta_p90_sec"),
                    "next_type": nxt,
                    "type_conf": conf,
                    "now_ts": now_ts,
                    "mc_status": mc_status,
                    "model_version": model_version,
                    "feature_version": feature_version,
                }
                buf_deque.append(row)
                if pred_id:
                    seen_ids.add(pred_id)
                added += 1
            except Exception:
                continue

    cutoff = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(hours=max_age_hours)
    while buf_deque and parse_ts(buf_deque[0]["eta_p50_ts"]) < cutoff:
        oldest = buf_deque.popleft()
        seen_ids.discard(oldest.get("pred_id"))

    return added


def color_badge(text: str, color_hex: str) -> str:
    return f"""
    <span style="background-color:{color_hex};color:white;padding:3px 8px;
        border-radius:12px;font-size:0.85rem;font-weight:600;">
        {str(text).upper()}
    </span>
    """


def format_time(ts: pd.Timestamp) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    now = pd.Timestamp.now(tz=ts.tz)
    if ts.date() == now.date():
        return ts.strftime("%H:%M:%S")
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------
st.set_page_config(page_title="Global Machine Alerts", page_icon="⏱️", layout="wide")

st.title("⏱️ Global Upcoming Machine Alerts")
st.caption(
    "Shows predicted next target machine status and ETA. "
    "Works with any process as long as prediction messages include process/machine/status fields."
)

with st.sidebar:
    st.subheader("Settings")
    st.text(f"Kafka: {BOOTSTRAP}")
    st.text(f"Topic: {TOPIC}")
    st.caption(f"Hidden statuses: {sorted(HIDE_STATUSES)}")

    start_mode = st.radio(
        "Start from",
        ["latest (live)", "earliest (replay)"],
        index=1,
        help="For replay/shadow test, choose earliest.",
    )
    offset_mode = "latest" if start_mode.startswith("latest") else "earliest"

    horizon_min = st.radio(
        "Horizon (minutes)",
        [30, 60, 120, 240, 1440],
        index={30: 0, 60: 1, 120: 2, 240: 3, 1440: 4}.get(DEFAULT_HORIZON_MIN, 1),
    )
    conf_thresh = st.slider("Show type if confidence ≥", 0.0, 1.0, DEFAULT_CONF_THRESH, 0.05)
    tz_choice = st.radio("Time zone", ["Local", "UTC"], index=0)
    refresh_sec = st.radio("Auto-refresh (sec)", [5, 10, 15], index={5: 0, 10: 1, 15: 2}.get(DEFAULT_REFRESH_SEC, 1))

    reset_reader = st.button("Reset and read from start (earliest)")
    show_hist = st.checkbox("Show historical predictions (ignore horizon)", value=(offset_mode == "earliest"))

if "buf" not in st.session_state:
    st.session_state.buf = deque(maxlen=100000)
if "seen" not in st.session_state:
    st.session_state.seen = set()
if "gid_suffix" not in st.session_state:
    st.session_state.gid_suffix = ""

if reset_reader and offset_mode == "earliest":
    st.session_state.gid_suffix = f"-{int(time.time())}"
    st.session_state.buf.clear()
    st.session_state.seen.clear()
    if "consumer" in st.session_state:
        del st.session_state["consumer"]

gid = GROUP_ID + (st.session_state.gid_suffix if offset_mode == "earliest" else "")

if (
    "consumer" not in st.session_state
    or st.session_state.get("offset_mode") != offset_mode
    or st.session_state.get("gid") != gid
):
    st.session_state.consumer = get_consumer(BOOTSTRAP, TOPIC, gid, offset_mode)
    st.session_state.offset_mode = offset_mode
    st.session_state.gid = gid
    st.session_state.buf.clear()
    st.session_state.seen.clear()

_ = ingest_from_consumer(st.session_state.consumer, st.session_state.buf, st.session_state.seen, max_age_hours=BUFFER_HOURS)

if st.session_state.buf:
    df = pd.DataFrame(list(st.session_state.buf))
    df["p50_utc"] = pd.to_datetime(df["eta_p50_ts"], utc=True, errors="coerce")
    df["p90_utc"] = pd.to_datetime(df["eta_p90_ts"], utc=True, errors="coerce")

    use_local = tz_choice == "Local"
    df["p50_disp"] = to_display_tz(df["p50_utc"], use_local)
    df["p90_disp"] = to_display_tz(df["p90_utc"], use_local)

    now_disp = pd.Timestamp.now(tz=df["p50_disp"].dt.tz if hasattr(df["p50_disp"].dt, "tz") else timezone.utc)
    horizon = now_disp + pd.Timedelta(minutes=int(horizon_min))

    df_live = df.copy() if show_hist else df.loc[(df["p50_disp"] >= now_disp) & (df["p50_disp"] <= horizon)].copy()

    # Process filter
    process_values = sorted([x for x in df_live.get("process", pd.Series(dtype=str)).dropna().unique().tolist()])
    if process_values:
        selected_processes = st.multiselect("Filter processes", options=process_values, default=process_values)
        if selected_processes:
            df_live = df_live[df_live["process"].isin(selected_processes)]

    machines = sorted(df_live["mc_no"].dropna().unique().tolist())
    selected = st.multiselect("Filter machines", options=machines, default=machines, help="Select machines to display")
    if selected:
        df_live = df_live[df_live["mc_no"].isin(selected)]

    def display_type(row):
        t = row.get("next_type")
        c = row.get("type_conf")
        if t is None or (pd.notna(c) and float(c) < conf_thresh):
            return ("UNCERTAIN", UNCERTAIN_COLOR, None)
        return (str(t), type_color(str(t)), float(c) if pd.notna(c) else None)

    st.markdown("### ⚡ Soonest Actionable Alerts")

    if df_live.empty:
        st.info("No predictions in the selected view. Try increasing the horizon or switching to replay mode.")
    else:
        res = df_live.apply(display_type, axis=1)
        disp_df = pd.DataFrame(res.tolist(), index=df_live.index, columns=["type_disp", "type_color", "conf_val"])
        df_live = pd.concat([df_live, disp_df], axis=1)

        df_live["p50_bucket"] = df_live["p50_disp"].dt.floor("1min")
        dedup_keys = ["process", "mc_no", "p50_bucket", "type_disp"] if "process" in df_live.columns else ["mc_no", "p50_bucket", "type_disp"]
        df_live_dedup = df_live.sort_values("p50_disp").drop_duplicates(subset=dedup_keys, keep="first")

        soonest = df_live_dedup.sort_values("p50_disp").head(10).copy()
        for _, row in soonest.iterrows():
            col1, col2, col3, col4, col5, col6 = st.columns([1.0, 1.2, 1.2, 2, 2, 1.2])
            with col1:
                st.markdown(f"**{row.get('process') or '—'}**")
            with col2:
                st.markdown(f"**{row['mc_no']}**")
            with col3:
                st.markdown(color_badge(row["type_disp"], row["type_color"]), unsafe_allow_html=True)
            with col4:
                st.markdown(f"around **{format_time(row['p50_disp'])}**")
            with col5:
                st.caption(f"safe by {format_time(row['p90_disp'])} (P90)" if pd.notna(row["p90_disp"]) else "")
            with col6:
                st.caption(f"conf {row['conf_val']:.2f}" if pd.notna(row["conf_val"]) else "conf —")

        st.markdown("### 📋 Earliest per Process + Machine")
        group_cols = ["process", "mc_no"] if "process" in df_live.columns else ["mc_no"]
        per_mc_earliest = df_live.sort_values("p50_disp").groupby(group_cols, as_index=False).first().sort_values("p50_disp")
        show_cols = [c for c in ["process", "mc_no", "type_disp", "p50_disp", "p90_disp", "conf_val", "mc_status"] if c in per_mc_earliest.columns]
        show_df = per_mc_earliest[show_cols].copy()
        show_df = show_df.rename(columns={
            "process": "Process",
            "mc_no": "Machine",
            "mc_status": "Current status",
            "type_disp": "Likely type",
            "p50_disp": "Around (P50)",
            "p90_disp": "Up to (P90)",
            "conf_val": "Conf",
        })
        if "Around (P50)" in show_df.columns:
            show_df["Around (P50)"] = show_df["Around (P50)"].apply(format_time)
        if "Up to (P90)" in show_df.columns:
            show_df["Up to (P90)"] = show_df["Up to (P90)"].apply(format_time)
        if "Conf" in show_df.columns:
            show_df["Conf"] = show_df["Conf"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "—")
        st.dataframe(show_df, use_container_width=True, hide_index=True)

        with st.expander("📊 Timeline"):
            plot_cols = ["process", "mc_no", "p50_disp", "type_disp"]
            plot_df = df_live_dedup[[c for c in plot_cols if c in df_live_dedup.columns]].copy()
            plot_df = plot_df.dropna(subset=["mc_no", "p50_disp", "type_disp"])
            plot_df = plot_df.sort_values("p50_disp").tail(PLOT_MAX_ROWS)
            plot_df["type_disp"] = plot_df["type_disp"].astype(str)
            plot_df["mc_no"] = plot_df["mc_no"].astype(str)
            if "process" in plot_df.columns:
                plot_df["machine_label"] = plot_df["process"].astype(str) + " / " + plot_df["mc_no"].astype(str)
            else:
                plot_df["machine_label"] = plot_df["mc_no"].astype(str)

            if not plot_df.empty:
                chart = (
                    alt.Chart(plot_df)
                    .mark_circle(size=80, opacity=0.85)
                    .encode(
                        x=alt.X("p50_disp:T", title="Around (P50)"),
                        y=alt.Y("machine_label:N", title="Process / Machine", sort=None),
                        color=alt.Color("type_disp:N", title="Type"),
                        tooltip=["machine_label", "type_disp", alt.Tooltip("p50_disp:T", title="Around (P50)")],
                    )
                    .properties(height=400)
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.caption("No points to plot.")
else:
    st.info("Waiting for predictions…")

st.markdown("---")
st.markdown("#### What do P50 and P90 mean?")
st.write(
    "- **P50:** best/typical ETA estimate.\n"
    "- **P90:** safer upper ETA estimate.\n"
    "- **UNCERTAIN:** predicted type is hidden because confidence is below threshold.\n"
    "- Hidden statuses are controlled by the `HIDE_STATUSES` environment variable."
)

with st.sidebar:
    st.markdown("---")
    st.subheader("Buffer stats")
    st.text(f"Buffered: {len(st.session_state.buf)} predictions")
    st.text(f"Unique IDs: {len(st.session_state.seen)}")

st.caption(f"Auto-refresh every {refresh_sec}s")
time.sleep(refresh_sec)
st.rerun()
