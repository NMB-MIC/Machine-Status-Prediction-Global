# Phase 5 Global Inference Runbook

Prereqs:

* Docker, kind, kubectl installed
* Kafka folder exists: `kafka/`
* Model artifacts are already placed under `service/models/`
* Required files inside `service/models/`:

  * `artifacts_phase4.pkl`
  * `lgbm_quantile_p50_final.pkl`
  * `lgbm_quantile_p90_final.pkl`
  * `lgbm_next_type.pkl`

This Phase 5 stack supports multiple processes such as `assy` and `gd`, as long as the correct artifacts are placed in `service/models/`.

Pipeline:

```text
Replay / MQTT
→ Kafka raw topic
→ Flink feature builder
→ FastAPI model service
→ Kafka prediction topic
→ Logger / Dashboard / Live Monitor
```

---

## 0) Clean old test runtime

From project root:

```bash
cd ~/Documents/P5_inference

pkill -f "python job.py" || true
pkill -f "PythonGatewayServer" || true
pkill -f "pyflink.fn_execution" || true
pkill -f "streamlit run app.py" || true
pkill -f "python.*live_monitor.py" || true
pkill -f "kafka-console-consumer" || true
pkill -f "kafka-console-producer" || true

docker rm -f pred_logger_test || true
docker rm -f pred_logger || true
```

Check:

```bash
ps aux | egrep "python job.py|PythonGatewayServer|pyflink|streamlit|live_monitor.py|kafka-console" | grep -v grep || true
```

No output = clean.

---

## 1) Start Kafka

```bash
cd kafka

docker compose up -d
chmod +x create-topics.sh
./create-topics.sh localhost:9092
```

Verify topics:

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected:

```text
iot.machine.status.raw
ml.pred.alert.eta
iot.machine.status.dlq
```

---

## 2) Start kind and namespace

```bash
kind get clusters
kind create cluster --name kind
```

If `kind` already exists, skip create.

Create namespace:

```bash
kubectl create namespace ml --dry-run=client -o yaml | kubectl apply -f -
kubectl get nodes -o wide
kubectl get ns
```

Expected:

* `kind-control-plane` is `Ready`
* namespace `ml` exists

---

## 3) Build service image

Make sure artifacts are already in:

```text
service/models/
```

Build:

```bash

docker build --no-cache \
  -t alert-eta-service:phase5 \
  -f service/Dockerfile \
  service
```

Quick model-load test:

```bash
docker run --rm -i alert-eta-service:phase5 \
  python - <<'PY'
import joblib
import pandas as pd
import sklearn
import pyarrow
import lightgbm

print("pandas", pd.__version__)
print("sklearn", sklearn.__version__)
print("pyarrow OK")
print("lightgbm OK")

a = joblib.load("/models/artifacts_phase4.pkl")
print("artifact loaded OK")
print("process_id:", a.get("process_id"))
print("feature_count:", len(a.get("feature_contract", {}).get("feature_cols", [])))
print("normal_statuses:", a.get("status_config", {}).get("normal_statuses"))
print("target_statuses:", a.get("status_config", {}).get("target_statuses"))
PY
```

Expected:

* `artifact loaded OK`
* `process_id` matches the artifact process, e.g. `assy` or `gd` or any other processes
* `feature_count` > 0

---

## 4) Load image into kind

```bash
kind load docker-image alert-eta-service:phase5 --name kind
```

Verify:

```bash
docker exec -it kind-control-plane crictl images | grep alert-eta-service || true
```

Expected:

```text
alert-eta-service    phase5
```

---

## 5) Deploy service

Deploy:

```bash
kubectl -n ml delete deployment alert-eta-service || true
kubectl -n ml delete svc alert-eta-service || true

kubectl apply -n ml -f k8s-kind/deployment.yaml
kubectl -n ml rollout status deployment/alert-eta-service --timeout=180s
```

Verify:

```bash
kubectl -n ml get pods -o wide
kubectl -n ml get svc -o wide
```

Expected:

* Pod = `Running 1/1`
* Service = `8080:30080/TCP`

---

## 6) Check service

Find kind IP:

```bash
KIND_IP=$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
echo $KIND_IP
```

Health:

```bash
curl -s http://$KIND_IP:30080/health | python3 -m json.tool
```

Metadata:

```bash
curl -s http://$KIND_IP:30080/metadata | python3 -m json.tool
```

Expected:

* `"status": "ok"`
* `"artifact_schema": "new"`
* `"process_id"` matches current artifacts
* feature count > 0
* normal statuses and target statuses exist

---

## 7) Clean Kafka topics before test

```bash
cd ~/Documents/P5_inference

docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic iot.machine.status.raw || true
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ml.pred.alert.eta || true
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic iot.machine.status.dlq || true

sleep 5

cd kafka
./create-topics.sh localhost:9092
cd ..
```

Verify:

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## 8) Run Flink job

Open a new terminal:

```bash
cd flink

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP=localhost:9092
export SERVICE_URL=http://$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}'):30080/infer
export SERVICE_METADATA_URL=http://$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}'):30080/metadata

python job.py
```

Expected:

```text
FeatureBuilder metadata: ...
```

Leave this terminal running.

---

## 9) Manual one-message test

Open another terminal.

Producer:

```bash
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot.machine.status.raw \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Send one event.

ASSY example:

```json
ffl-07-2:{"event_id":"manual-assy-test-1","plant":"nhb","process":"assy","mc_no":"ffl-07-2","occurred_ts":"2026-02-18T10:00:00Z","mc_status":"alarm","ingest_ts":"2026-02-18T10:00:01Z","schema_version":1}
```

GD example:

```json
ic03r:{"event_id":"manual-gd-test-1","plant":"nht","process":"gd","mc_no":"ic03r","occurred_ts":"2026-05-11T20:00:00Z","mc_status":"mc_waitpart","ingest_ts":"2026-05-11T20:00:01Z","schema_version":1}
```

Press Enter, then Ctrl+C.

Consume prediction:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ml.pred.alert.eta \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000
```

Expected prediction fields:

* `eta_p50_sec`
* `eta_p90_sec`
* `next_type`
* `type_conf`
* `plant`
* `process`
* `mc_no`
* `mc_status`

Check DLQ:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot.machine.status.dlq \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000 || true
```

Expected:

```text
Processed a total of 0 messages
```

---

## 10) Replay dataset for shadow test

From another terminal:

```bash
cd replay

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

ASSY example:

```bash
python replay.py \
  --input /home/micml/Documents/TestML/AutoTrain/nht_assy/MCSTATUS_ASSY.csv \
  --process-override assy \
  --plant-override nhb \
  --bootstrap localhost:9092 \
  --topic iot.machine.status.raw \
  --limit 10000 \
  --sleep 0.0
```

GD example:

```bash
python replay.py \
  --input /home/micml/Documents/TestML/AutoTrain/nht_gd/status_nht_converted.parquet \
  --process-override gd \
  --plant-override nht \
  --bootstrap localhost:9092 \
  --topic iot.machine.status.raw \
  --limit 10000 \
  --sleep 0.0
```

Expected:

```text
Replay finished: 10000 messages
```

---

## 11) Validate Kafka counts

Use Python, because some Kafka containers do not have `kafka-run-class.sh`.

```bash
cd replay
source venv/bin/activate

python - <<'PY'
from kafka import KafkaConsumer, TopicPartition

topics = [
    "iot.machine.status.raw",
    "ml.pred.alert.eta",
    "iot.machine.status.dlq",
]

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
)

for topic in topics:
    partitions = consumer.partitions_for_topic(topic)
    print(f"---- {topic} ----")

    if not partitions:
        print("NO_PARTITIONS")
        continue

    tps = [TopicPartition(topic, p) for p in sorted(partitions)]
    beginning = consumer.beginning_offsets(tps)
    end = consumer.end_offsets(tps)

    total = 0
    for tp in tps:
        count = end[tp] - beginning[tp]
        total += count
    print("TOTAL=", total)

consumer.close()
PY
```

Expected after replay catches up:

```text
iot.machine.status.raw    TOTAL=10000
ml.pred.alert.eta         TOTAL=10000
iot.machine.status.dlq    TOTAL=0
```

If prediction count is still low or 0, wait 10–30 seconds and run count again.

---

## 12) Validate prediction samples

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ml.pred.alert.eta \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 20000
```

Expected:

* JSON predictions appear
* `process` and `plant` are correct
* `eta_p50_sec` and `eta_p90_sec` exist
* `eta_p50_sec` should be less than or equal to `eta_p90_sec`

Check DLQ:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot.machine.status.dlq \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 10000 || true
```

Expected:

```text
Processed a total of 0 messages
```

---

## 13) Start prediction logger

Build logger:

```bash
cd Predictions/pred_logger

docker build --no-cache -t mic/pred_logger:global-flush .
```

Start logger:

```bash
docker rm -f pred_logger || true

docker run -d \
  --name pred_logger \
  --network host \
  --user "$(id -u):$(id -g)" \
  -e KAFKA_BOOTSTRAP=localhost:9092 \
  -e KAFKA_TOPIC=ml.pred.alert.eta \
  -e KAFKA_GROUP_ID=pred-logger \
  -e AUTO_OFFSET_RESET=latest \
  -e OUTPUT_DIR=/data/predictions \
  -e FLUSH_EVERY=1000 \
  -e FLUSH_SECS=60 \
  -v /home/micml/Documents/TestML/predictions:/data/predictions \
  mic/pred_logger:global-flush
```

Watch:

```bash
docker logs pred_logger -f
```

Files stored at:

```text
/home/micml/Documents/TestML/predictions/predictions_YYYY-MM-DD.parquet
```

For backfill/replay test from earliest, use a unique group and `AUTO_OFFSET_RESET=earliest`.

---

## 14) Start dashboard

```bash
cd dashboard

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=ml.pred.alert.eta
export BUFFER_HOURS=87600
export HIDE_STATUSES="run,mc_run,no_work,no work"
export TYPE_CONF_THRESHOLD=0.6
export REFRESH_SEC=5

streamlit run app.py --server.port 8503
```

Open:

```text
http://localhost:8503
```

Expected:

* Dashboard loads
* Process filter works
* Machine filter works
* Alert table appears
* Timeline appears

---

## 15) Start live monitor

```bash
cd monitor

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=ml.pred.alert.eta
export TYPE_CONF_THRESHOLD=0.6
export HIDE_STATUSES="run,mc_run,no_work,no work"

python live_monitor.py
```

Expected:

```text
GLOBAL LIVE PREDICTION MONITOR
process | machine | status | ETA | P90 | type | alert_at
```

If silent, replay 3 fresh rows.

---

## 16) Start MQTT bridge for live data

Only use this for live machine data.

```bash
cd mqtt_to_ml_kafka
```

Edit `.env`:

* `MQTT_BROKER`
* `MQTT_PORT`
* `MQTT_TOPIC`
* `KAFKA_SERVER`
* `KAFKA_TOPIC`

Build and run:

```bash
docker build --no-cache -t mic/mqtt_ml_kafka:global .
docker compose up -d
docker logs mqtt_to_kafka -f
```

Expected:

* MQTT connected
* Kafka connected
* Messages published to `iot.machine.status.raw`

---

## 17) Live deployment sequence

For real live mode:

```text
1. Start Kafka
2. Start kind
3. Deploy service
4. Start Flink
5. Start prediction logger
6. Start MQTT bridge
7. Start live monitor
8. Start dashboard
```

If predictions appear in monitor and dashboard, live stack is working.

---

## 18) Acceptance checklist

Phase 5 passes if:

```text
/health returns ok
/metadata returns correct process_id
Flink prints metadata
Replay sends 10000 messages
Raw topic count = 10000
Prediction topic count = 10000
DLQ count = 0
Prediction JSON includes plant/process/ETA fields
Logger writes parquet
Dashboard renders predictions
Live monitor prints predictions
```

---

## 19) Troubleshooting

Pod image pull error:

```bash
grep -n "image:\|imagePullPolicy" k8s-kind/deployment.yaml
docker exec -it kind-control-plane crictl images | grep alert-eta-service || true
```

Fix:

```bash
kind load docker-image alert-eta-service:phase5 --name kind
kubectl -n ml delete pod -l app=alert-eta-service
```

Pod crashing:

```bash
kubectl -n ml logs deployment/alert-eta-service --tail=100
```

No predictions:

* Check Flink terminal
* Check service health
* Check raw topic has messages
* Check DLQ

```bash
curl http://$KIND_IP:30080/health
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot.machine.status.dlq --from-beginning --max-messages 10
```

Kafka consumer timeout:

* Not always bad.
* If it says `Processed a total of 0 messages`, the topic is empty.

Artifact load errors:

* Make sure service requirements include:

  * `pandas==2.3.3`
  * `scikit-learn==1.7.2`
  * `pyarrow`

Replay CSV date warning:

* `replay.py` should parse occurred with `dayfirst=True`.

Logger misses final rows:

* Use `mic/pred_logger:global-flush`.
* It flushes idle buffered rows.

---

## 20) Stop test runtime

Stop Flink in its terminal with Ctrl+C.

Then:

```bash
docker rm -f pred_logger_test || true
docker rm -f pred_logger || true

pkill -f "streamlit run app.py" || true
pkill -f "python.*live_monitor.py" || true
pkill -f "python job.py" || true
pkill -f "PythonGatewayServer" || true
pkill -f "pyflink.fn_execution" || true
```

Check:

```bash
ps aux | egrep "streamlit|live_monitor.py|python job.py|flink" | grep -v grep || true
```

Kafka and kind may stay running if still testing.

Fully stop kind:

```bash
kind delete cluster --name kind
```

Fully stop Kafka test stack:

```bash
cd ~/Documents/P5_inference/kafka
docker compose down -v --remove-orphans
```
