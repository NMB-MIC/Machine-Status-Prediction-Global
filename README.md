# Machine Status Prediction Global

Global Phase 5 real-time machine status prediction system.

This project is the generalized version of the original ASSY-only predictive maintenance inference system.
It supports different machine processes such as `assy`, `gd`, or other future processes as long as the correct Phase 4 model artifacts are placed inside `service/models/`.

The system predicts:

* Estimated time to next abnormal / target machine status
* P50 ETA
* P90 ETA
* Next predicted status type
* Type confidence
* Machine, process, and plant context

---

## 1. System Overview

Pipeline:

```text
Machine status event
→ MQTT bridge or replay producer
→ Kafka raw topic
→ PyFlink feature builder
→ FastAPI model service
→ Kafka prediction topic
→ Prediction logger / Dashboard / Live monitor
```

Main Kafka topics:

```text
iot.machine.status.raw      # raw machine status events
ml.pred.alert.eta           # prediction output
iot.machine.status.dlq      # failed / bad events
```

---

## 2. What This Global Version Adds

Compared with the original ASSY V1 version, this global version adds:

* Dynamic process support
* Dynamic status support from Phase 4 artifacts
* `/metadata` endpoint for Flink to fetch model configuration
* Process-aware feature building
* Plant/process fields in prediction output
* Global replay support for CSV and Parquet
* Global dashboard filtering by process and machine
* Global prediction logger with plant/process columns
* Global live monitor
* Fixed logger idle flush so final buffered predictions are not lost

---

## 3. Repository Structure

```text
.
├── kafka/                    # Kafka + topic creation
├── service/                  # FastAPI model inference service
│   └── models/               # Place Phase 4 artifacts here
├── flink/                    # PyFlink streaming feature builder
├── replay/                   # CSV / Parquet replay into Kafka
├── dashboard/                # Streamlit prediction dashboard
├── monitor/                  # Terminal live prediction monitor
├── Predictions/
│   └── pred_logger/          # Prediction logger to Parquet
├── mqtt_to_ml_kafka/         # MQTT → Kafka bridge for live mode
└── k8s-kind/                 # Kubernetes manifests for local kind cluster
```

---

## 4. Required Model Artifacts

Before building the service image, place these 4 files inside:

```text
service/models/
```

Required files:

```text
artifacts_phase4.pkl
lgbm_quantile_p50_final.pkl
lgbm_quantile_p90_final.pkl
lgbm_next_type.pkl
```

The artifact determines which process is currently deployed.

Example:

```text
ASSY artifacts → process_id = assy
GD artifacts   → process_id = gd
```

To switch process, replace the 4 artifact files inside `service/models/`, rebuild the service image, and redeploy.

---

## 5. Prerequisites

Installed on server:

```text
Docker
kind
kubectl
Python 3.10+
Java 11+
```

For PyFlink, make sure the Kafka connector jars are available in the Flink environment.

---

## 6. Quick Start

### 6.1 Start Kafka

```bash
cd kafka
docker compose up -d
chmod +x create-topics.sh
./create-topics.sh localhost:9092
```

Verify topics:

```bash
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected:

```text
iot.machine.status.raw
ml.pred.alert.eta
iot.machine.status.dlq
```

---

### 6.2 Start kind and namespace

```bash
kind get clusters
kind create cluster --name kind
```

If `kind` already exists, skip create.

```bash
kubectl create namespace ml --dry-run=client -o yaml | kubectl apply -f -
kubectl get nodes -o wide
```

Expected:

```text
kind-control-plane   Ready
```

---

### 6.3 Build service image

Make sure model artifacts already exist in:

```text
service/models/
```

Build:

```bash
cd service
docker build --no-cache -t alert-eta-service:phase5 .
cd ..
```

Load image into kind:

```bash
kind load docker-image alert-eta-service:phase5 --name kind
```

---

### 6.4 Deploy service

Make sure `k8s-kind/deployment.yaml` uses:

```yaml
image: alert-eta-service:phase5
imagePullPolicy: IfNotPresent
```

Deploy:

```bash
kubectl -n ml delete deployment alert-eta-service || true
kubectl -n ml delete svc alert-eta-service || true

kubectl apply -n ml -f k8s-kind/deployment.yaml
kubectl -n ml rollout status deployment/alert-eta-service --timeout=180s
```

Verify:

```bash
kubectl -n ml get pods
kubectl -n ml get svc
```

Expected:

```text
Pod READY = 1/1
Service NodePort = 8080:30080
```

---

### 6.5 Check service

Get kind IP:

```bash
KIND_IP=$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
echo $KIND_IP
```

Check health:

```bash
curl -s http://$KIND_IP:30080/health | python3 -m json.tool
```

Check metadata:

```bash
curl -s http://$KIND_IP:30080/metadata | python3 -m json.tool
```

Expected:

```text
status = ok
artifact_schema = new
process_id = current deployed process
normal_statuses = loaded from artifact
target_statuses = loaded from artifact
feature_count > 0
```

---

## 7. Run PyFlink Job

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

Expected output:

```text
FeatureBuilder metadata: ...
```

Leave this terminal running.

---

## 8. Manual Test

Open another terminal.

Start producer:

```bash
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot.machine.status.raw \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Example ASSY event:

```json
ffl-07-2:{"event_id":"manual-assy-test-1","plant":"nhb","process":"assy","mc_no":"ffl-07-2","occurred_ts":"2026-02-18T10:00:00Z","mc_status":"alarm","ingest_ts":"2026-02-18T10:00:01Z","schema_version":1}
```

Example GD event:

```json
ic03r:{"event_id":"manual-gd-test-1","plant":"nht","process":"gd","mc_no":"ic03r","occurred_ts":"2026-05-11T20:00:00Z","mc_status":"mc_waitpart","ingest_ts":"2026-05-11T20:00:01Z","schema_version":1}
```

Consume prediction:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ml.pred.alert.eta \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000
```

If prediction JSON appears, Flink + service works.

Expected output fields:

```text
pred_id
source_event_id
plant
process
mc_no
mc_status
eta_p50_sec
eta_p90_sec
eta_p50_ts
eta_p90_ts
next_type
type_conf
model_version
feature_version
```

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

## 9. Replay Test

Replay is used for shadow mode or local validation.

### ASSY replay example

```bash
cd replay
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python replay.py \
  --input /home/micml/Documents/TestML/AutoTrain/nht_assy/MCSTATUS_ASSY.csv \
  --process-override assy \
  --plant-override nhb \
  --bootstrap localhost:9092 \
  --topic iot.machine.status.raw \
  --limit 10000 \
  --sleep 0.0
```

### GD replay example

```bash
cd replay
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

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

## 10. Validate Topic Counts

Use Python counting because some Kafka containers do not include `kafka-run-class.sh`.

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
        total += end[tp] - beginning[tp]

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

## 11. Prediction Logger

The logger stores predictions as daily Parquet files.

Build:

```bash
cd Predictions/pred_logger
docker build --no-cache -t mic/pred_logger:global-flush .
```

Run:

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

Watch logs:

```bash
docker logs pred_logger -f
```

Output path:

```text
/home/micml/Documents/TestML/predictions/predictions_YYYY-MM-DD.parquet
```

---

## 12. Dashboard

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

Dashboard should show:

* Process filter
* Machine filter
* Alert table
* Prediction timeline
* ETA / P90 values
* Next type and confidence

---

## 13. Live Monitor

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

If monitor is silent, replay a few fresh rows.

---

## 14. MQTT Bridge for Live Data

Use this only for live machine data.

```bash
cd mqtt_to_ml_kafka
```

Edit `.env`:

```text
MQTT_BROKER=
MQTT_PORT=
MQTT_TOPIC=
KAFKA_SERVER=localhost:9092
KAFKA_TOPIC=iot.machine.status.raw
```

Build and run:

```bash
docker build --no-cache -t mic/mqtt_ml_kafka:global .
docker compose up -d
docker logs mqtt_to_kafka -f
```

Expected:

```text
MQTT connected
Kafka connected
messages published to iot.machine.status.raw
```

---

## 15. Live Deployment Sequence

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

If predictions appear in live monitor and dashboard, the live stack is working.

---

## 16. Acceptance Checklist

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

## 17. Troubleshooting

Pod crashing:

```bash
kubectl -n ml logs deployment/alert-eta-service --tail=100
```

Image pull error:

```bash
grep -n "image:\|imagePullPolicy" k8s-kind/deployment.yaml
docker exec -it kind-control-plane crictl images | grep alert-eta-service || true
```

Fix:

```bash
kind load docker-image alert-eta-service:phase5 --name kind
kubectl -n ml delete pod -l app=alert-eta-service
```

No predictions:

```bash
curl http://$KIND_IP:30080/health

docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot.machine.status.dlq \
  --from-beginning \
  --max-messages 10
```

Also check:

```text
Flink terminal
Service logs
Raw topic has messages
DLQ messages
```

Kafka timeout:

```text
TimeoutException is not always bad.
If it says Processed a total of 0 messages, the topic is empty.
```

Artifact load errors:

```text
Make sure service requirements include:
pandas==2.3.3
scikit-learn==1.7.2
pyarrow
```

Logger misses final rows:

```text
Use mic/pred_logger:global-flush.
It flushes leftover buffered rows during idle time.
```

---

## 18. Stop Test Runtime

Stop Flink terminal with:

```text
Ctrl+C
```

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

Stop kind:

```bash
kind delete cluster --name kind
```

Stop Kafka:

```bash
cd kafka
docker compose down -v --remove-orphans
```

---

## 19. Notes

* The deployed process depends on the artifacts inside `service/models/`.
* To switch process, replace the 4 artifact files and rebuild the service image.
* Flink does not hardcode ASSY or GD statuses.
* Flink fetches status configuration from `/metadata`.
* Prediction output always includes `plant` and `process`.
* The dashboard and logger are global-process aware.
