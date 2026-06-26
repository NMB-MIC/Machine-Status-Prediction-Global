# monitor/live_monitor.py — Terminal live prediction monitor, global version

import json
import os
from datetime import datetime
from kafka import KafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "ml.pred.alert.eta")
HIDE = {s.strip().lower().replace(" ", "_") for s in os.environ.get("HIDE_STATUSES", "run,mc_run,no_work,no work").split(",") if s.strip()}
CONF_THR = float(os.environ.get("TYPE_CONF_THRESHOLD", "0.6"))
OFFSET_MODE = os.environ.get("OFFSET_MODE", "latest")


def norm(x):
    if x is None:
        return None
    return str(x).strip().lower().replace(" ", "_").replace("/", "_")


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=f"live-monitor-{int(datetime.now().timestamp())}",
    auto_offset_reset=OFFSET_MODE,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=-1,
)

print(f"{'='*100}")
print("  GLOBAL LIVE PREDICTION MONITOR")
print(f"  Topic: {TOPIC} | Bootstrap: {BOOTSTRAP} | Offset: {OFFSET_MODE}")
print(f"  Hiding: {sorted(HIDE)} | Conf threshold: {CONF_THR}")
print(f"{'='*100}\n")

count = 0
shown = 0

for msg in consumer:
    count += 1
    p = msg.value

    plant = p.get("plant") or "-"
    process = p.get("process") or "-"
    mc_no = p.get("mc_no", "?")
    mc_status = p.get("mc_status", "?")
    p50 = float(p.get("eta_p50_sec") or 0)
    p90 = float(p.get("eta_p90_sec") or 0)
    p50_ts = p.get("eta_p50_ts", "")
    nxt = p.get("next_type")
    conf = p.get("type_conf")

    if nxt and norm(nxt) in HIDE:
        continue

    shown += 1

    if nxt and conf is not None and float(conf) >= CONF_THR:
        type_str = f"{nxt} (p={float(conf):.2f})"
    elif nxt is None:
        type_str = "[uncertain]"
    else:
        type_str = f"[hidden, p={float(conf):.2f}]"

    eta_min = p50 / 60
    p90_min = p90 / 60

    print(
        f"{process:<8} | {mc_no:<12} | "
        f"status={str(mc_status):<14} | "
        f"ETA={eta_min:6.1f}m (P90={p90_min:6.1f}m) | "
        f"type={type_str:<28} | "
        f"alert_at={str(p50_ts)[:19]}"
    )

    if shown % 50 == 0:
        print(f"\n--- {shown} shown / {count} total predictions ---\n")
