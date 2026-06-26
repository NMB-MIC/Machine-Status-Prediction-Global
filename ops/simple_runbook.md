# Phase 5 Global Inference Runbook (Simplified)

### Prereqs

* Docker, kind, kubectl installed
* Model artifacts placed under `service/models/` (`artifacts_phase4.pkl`, `lgbm_quantile_p50_final.pkl`, etc.)

---

### 0) Clean Environment

```bash
pkill -f "python job.py|PythonGatewayServer|pyflink|streamlit|live_monitor.py|kafka-console" || true
docker rm -f pred_logger pred_logger_test || true

```

### 1) Start Kafka

```bash
cd kafka
docker compose up -d
./create-topics.sh localhost:9092
# Verify
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

```

### 2) Start kind and namespace

```bash
kind get clusters || kind create cluster --name kind
kubectl create namespace ml --dry-run=client -o yaml | kubectl apply -f -

```

### 3) Build and load service image

```bash
cd service
docker build --no-cache -t alert-eta-service:phase5 .
kind load docker-image alert-eta-service:phase5 --name kind

```

### 4) Deploy service

```bash
kubectl -n ml delete deployment alert-eta-service || true
kubectl apply -n ml -f k8s-kind/deployment.yaml
kubectl -n ml rollout status deployment/alert-eta-service --timeout=180s
kubectl -n ml get svc   # Verify PORT=8080:30080

```

### 5) Check service

```bash
KIND_IP=$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
curl -s http://$KIND_IP:30080/health
curl -s http://$KIND_IP:30080/metadata

```

### 6) Run PyFlink job

```bash
cd flink
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP=localhost:9092
export SERVICE_URL=http://$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}'):30080/infer
export SERVICE_METADATA_URL=http://$(docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}'):30080/metadata
python job.py

```

> **Test if Flink works:**
> In a new terminal, send a test message:
> ```bash
> docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot.machine.status.raw --property "parse.key=true" --property "key.separator=:"
> # Paste ASSY example:
> ffl-07-2:{"event_id":"manual-test","plant":"nhb","process":"assy","mc_no":"ffl-07-2","occurred_ts":"2026-02-18T10:00:00Z","mc_status":"alarm","ingest_ts":"2026-02-18T10:00:01Z","schema_version":1}
> 
> ```
> 
> 
> In another terminal, verify prediction output:
> ```bash
> docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ml.pred.alert.eta --from-beginning --max-messages 1
> 
> ```
> 
> 

### 7) Replay for shadow mode (optional testing)

```bash
cd replay
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# For ASSY:
python replay.py --input /home/micml/Documents/TestML/AutoTrain/nht_assy/MCSTATUS_ASSY.csv --process-override assy --plant-override nhb --bootstrap localhost:9092 --topic iot.machine.status.raw --limit 10000 --sleep 0.0

# For GD:
python replay.py --input /home/micml/Documents/TestML/AutoTrain/nht_gd/status_nht_converted.parquet --process-override gd --plant-override nht --bootstrap localhost:9092 --topic iot.machine.status.raw --limit 10000 --sleep 0.0

```

### 8) Validate predictions

```bash
# Check predictions stream
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ml.pred.alert.eta --from-beginning --max-messages 10
# Check DLQ (Should be empty/0 messages)
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot.machine.status.dlq --from-beginning --timeout-ms 5000

```

### 9) Start prediction logger

```bash
cd Predictions/pred_logger
docker build --no-cache -t mic/pred_logger:global-flush .

docker run -d --name pred_logger --network host --user "$(id -u):$(id -g)" \
  -e KAFKA_BOOTSTRAP=localhost:9092 -e KAFKA_TOPIC=ml.pred.alert.eta \
  -e KAFKA_GROUP_ID=pred-logger -e AUTO_OFFSET_RESET=latest \
  -e OUTPUT_DIR=/data/predictions -e FLUSH_EVERY=1000 -e FLUSH_SECS=60 \
  -v /home/micml/Documents/TestML/predictions:/data/predictions \
  mic/pred_logger:global-flush

docker logs pred_logger -f  # verify flushing

```

### 10) Dashboard

```bash
cd dashboard
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=ml.pred.alert.eta
export BUFFER_HOURS=87600
export HIDE_STATUSES="run,mc_run,no_work,no work"
export TYPE_CONF_THRESHOLD=0.6
export REFRESH_SEC=5
streamlit run app.py --server.port 8503

```

### 11) Live mode — MQTT bridge

```bash
cd mqtt_to_ml_kafka
# Edit .env to set correct MQTT_BROKER and KAFKA_SERVER IPs
docker build --no-cache -t mic/mqtt_ml_kafka:global .
docker compose up -d
docker logs mqtt_to_kafka -f

```

### 12) Terminal Live Prediction Viewer

```bash
cd monitor
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=ml.pred.alert.eta
export TYPE_CONF_THRESHOLD=0.6
export HIDE_STATUSES="run,mc_run,no_work,no work"
python live_monitor.py

```

---

### 13) Verification Sequence (Live Deployment)

1. Start Kafka, Kind, Service, Flink (Steps 1–6)
2. Start prediction logger (Step 9)
3. Start MQTT bridge (Step 11)
4. Start live monitor (Step 12)
5. Verify incoming data flow, then spin up Dashboard (Step 10)

### Acceptance Gates

* **SLA:** MedAE < ~60s; Hit@5m > 75%; Service p95 latency < 100ms; Error rate < 1%
* **Outputs:** Prediction JSON includes `plant`, `process`, `eta_p50_sec`, `eta_p90_sec`. `type_conf` fields hidden if < 0.6.

### Troubleshooting

* **Pod Pull Error:** Run `kind load docker-image alert-eta-service:phase5 --name kind` and delete the stalled pod to force recreation.
* **Pod Crashing:** `kubectl -n ml logs deployment/alert-eta-service --tail=50`
* **No Predictions:** Check Flink terminal logs, verify `curl http://$KIND_IP:30080/health` returns ok, and make sure `iot.machine.status.dlq` isn't swallowing bad schemas.
* **Stale Test Data:** Clear everything out via `./create-topics.sh localhost:9092` to wipe topics cleanly.
