# replay/replay.py — Global CSV/Parquet/Excel replay to Kafka
# Sends machine status events using the Phase 5 global event schema.

import argparse
import time
import json
import os
import re
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timezone


def normalize_col(c: str) -> str:
    return str(c).strip()


def normalize_status_text(x) -> str:
    s = str(x).strip().lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", "_", s)
    s = s.replace("/", "_")
    s = s.replace("-", "_")
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def to_iso(ts):
    ts = pd.to_datetime(ts, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp: {ts}")
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def load_data(path: str, process_override=None, plant_override=None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".parquet":
        df = pd.read_parquet(path)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif ext in [".csv", ".tsv", ".txt"]:
        sep = "\t" if ext in [".tsv", ".txt"] else ","
        df = pd.read_csv(path, sep=sep)
    else:
        df = pd.read_csv(path)

    df.columns = [normalize_col(c) for c in df.columns]

    # Case-insensitive column recovery
    lower_map = {c.lower(): c for c in df.columns}
    rename = {}
    for required in ["occurred", "mc_no", "mc_status", "process", "plant"]:
        if required not in df.columns and required in lower_map:
            rename[lower_map[required]] = required
    if rename:
        df = df.rename(columns=rename)

    required = {"occurred", "mc_no", "mc_status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Got: {list(df.columns)}")

    if "process" not in df.columns:
        if not process_override:
            raise ValueError("Missing required column 'process'. Provide --process-override if replaying old files.")
        df["process"] = process_override

    if process_override:
        df["process"] = process_override

    if "plant" not in df.columns:
        df["plant"] = plant_override or None
    elif plant_override:
        df["plant"] = plant_override

    df = df[["occurred", "mc_no", "mc_status", "process", "plant"]].copy()
    df["occurred"] = pd.to_datetime(df["occurred"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["occurred", "mc_no", "mc_status", "process"])

    df["mc_no"] = df["mc_no"].astype(str).str.strip()
    df["process"] = df["process"].astype(str).str.strip().str.lower()
    df["plant"] = df["plant"].where(df["plant"].notna(), None)
    df["mc_status"] = df["mc_status"].astype(str).map(normalize_status_text)

    df = df.sort_values(["process", "mc_no", "occurred", "mc_status"]).reset_index(drop=True)
    return df


def main():
    ap = argparse.ArgumentParser(description="Replay machine status CSV/Parquet/Excel data to Kafka")
    ap.add_argument("--input", required=False, help="Path to CSV, Parquet, or Excel file")
    ap.add_argument("--parquet", required=False, help="Backward compatible alias for --input")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="iot.machine.status.raw")
    ap.add_argument("--sleep", type=float, default=0.0, help="Seconds between messages. 0 = full speed")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to send. 0 = all")
    ap.add_argument("--process-override", default=None, help="Use this process value if file has no process column or to force one process")
    ap.add_argument("--plant-override", default=None, help="Optional plant value to add/override")
    args = ap.parse_args()

    path = args.input or args.parquet
    if not path:
        ap.error("Must provide --input")

    print(f"Loading replay data from: {path}")
    df = load_data(path, process_override=args.process_override, plant_override=args.plant_override)

    if args.limit > 0:
        df = df.head(args.limit)

    print(json.dumps({
        "rows": int(len(df)),
        "machines": int(df["mc_no"].nunique()),
        "processes": sorted(df["process"].dropna().unique().tolist()),
        "plants": sorted([str(x) for x in df["plant"].dropna().unique().tolist()]),
        "statuses": sorted(df["mc_status"].dropna().unique().tolist()),
        "time_min": str(df["occurred"].min()),
        "time_max": str(df["occurred"].max()),
    }, indent=2, ensure_ascii=False))

    prod = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    sent = 0
    t0 = time.time()

    for i, row in df.iterrows():
        process = str(row["process"]).strip().lower()
        mc_no = str(row["mc_no"]).strip()
        occurred_iso = to_iso(row["occurred"])

        rec = {
            "event_id": f"{process}-{mc_no}-{i}",
            "plant": row.get("plant") if pd.notna(row.get("plant")) else None,
            "process": process,
            "mc_no": mc_no,
            "occurred_ts": occurred_iso,
            "mc_status": row["mc_status"],
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
            "schema_version": 2,
        }

        prod.send(args.topic, key=f"{process}||{mc_no}", value=rec)
        sent += 1

        if args.sleep > 0:
            time.sleep(args.sleep)

        if sent % 50000 == 0:
            elapsed = time.time() - t0
            rate = sent / max(elapsed, 0.001)
            print(f"Sent {sent}/{len(df)} ({rate:.0f} msg/s)")

    prod.flush()
    elapsed = time.time() - t0
    print(f"Replay finished: {sent} messages in {elapsed:.1f}s ({sent / max(elapsed, 0.001):.0f} msg/s)")


if __name__ == "__main__":
    main()
