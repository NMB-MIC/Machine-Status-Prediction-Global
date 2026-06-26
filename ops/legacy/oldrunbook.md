Prereqs:
- Docker, kind, kubectl installed on the server
- Artifacts placed under service/models/

1) Start Kafka (PLAINTEXT localhost)
- cd kafka
- docker compose up -d
- ./create-topics.sh
- Verify topics exist: docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

2) Start kind and namespace (skip if already running)
- kind get clusters                          # check first
- kind create cluster --name kind            # only if not listed
- kubectl create namespace ml --dry-run=client -o yaml | kubectl apply -f -

3) Build and load service image
- cd service
- docker build -t alert-eta-service:v2 .
- kind load docker-image alert-eta-service:v2 --name kind

4) Deploy service (NodePort on 30080)
- kubectl apply -n ml -f ../k8s-kind/deployment.yaml
- kubectl -n ml get svc    # verify TYPE=NodePort, PORT=8080:30080

5) Check service
- Find kind node IP:
    docker inspect kind-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}'
- kubectl -n ml get pods   # wait for READY 1/1
- curl http://172.18.0.2:30080/health
- Expected: {"status":"ok","model_version":"2025-01-15_p50p90_v2","feature_version":"feats_39_behavioral_v2","num_features":39,...}
- NOTE: No port-forward needed. NodePort exposes the service directly.

6) Run PyFlink job
- Ensure flink-connector-kafka jar is in $FLINK_HOME/lib
- cd flink
- python3 -m venv venv && . venv/bin/activate
- pip install -r requirements.txt
- Run with NodePort URL:
    python job.py

 Test if Flink works

- After running job.py without any problem, try:
- In a new terminal, try: docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot.machine.status.raw --property "parse.key=true" --property "key.separator=:"
- try send: ffl-07-2:{"event_id":"test-1","mc_no":"ffl-07-2","occurred_ts":"2026-02-18T10:00:00Z","mc_status":"alarm","ingest_ts":"2025-10-18T10:00:01Z","schema_version":1}
- In another terminal, try consume: docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ml.pred.alert.eta --from-beginning --max-messages 1
- If you see a JSON with "eta_p50_sec", "eta_p90_sec", "next_type" fields → Flink + Service pipeline works correctly! Proceed to next step.
- If nothing appears after ~10s, check:
  - kubectl -n ml get pods (pod must be Running 1/1)
  - curl http://172.18.0.2:30080/health returns ok
  - Flink logs for HTTP errors

7) Replay for shadow mode (optional, for testing)
- cd replay
- python3 -m venv venv && source venv/bin/activate
- pip install -r requirements.txt
- python replay.py --input /home/micml/Documents/TestML/DATA_MCSTATUS_ASSY_202601300925.csv --bootstrap localhost:9092 --topic iot.machine.status.raw --sleep 0.0

8) Validate predictions (consumer or UI)
- Consume the first 10 prediction records: docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ml.pred.alert.eta --from-beginning --max-messages 10
- Consume ml.pred.alert.eta and check fields:
  - P50 < P90, timestamps, type_conf policy, next_type/type_conf = null if conf < 0.6 (by design)
- Monitor service /metrics and Flink logs

9) Start prediction logger (stores predictions as daily Parquet files)
- cd pred_logger
- docker build --no-cache -t mic/pred_logger:1.0.0 .
- docker compose up -d
- docker logs pred_logger -f   # verify flushing
- Files stored at: /home/micml/Documents/TestML/predictions/predictions_YYYY-MM-DD.parquet
- View stored predictions:
    python view_predictions.py
    python view_predictions.py /home/micml/Documents/TestML/predictions/predictions_2026-03-06.parquet

10) Dashboard
- cd dashboard
- python3 -m venv venv && source venv/bin/activate
- pip install -r requirements.txt
- For replay:
    export KAFKA_BOOTSTRAP=localhost:9092
    export KAFKA_TOPIC=ml.pred.alert.eta
    export BUFFER_HOURS=87600
    streamlit run app.py --server.port 8503
- For live:
    export KAFKA_BOOTSTRAP=localhost:9092
    export KAFKA_TOPIC=ml.pred.alert.eta
    export BUFFER_HOURS=24
    streamlit run app.py --server.port 8503
- In sidebar, select "latest (live)" for live mode, "earliest (replay)" for replay mode

11) Live mode — MQTT bridge
- cd mqtt_to_ml_kafka
- Edit .env — set correct MQTT_BROKER IP and KAFKA_SERVER IP
- docker build --no-cache -t mic/mqtt_ml_kafka:1.0.0 .
- docker compose up -d
- docker logs mqtt_to_ml_kafka -f   # watch for "Connected" and message counts

12) Terminal Live Prediction Viewer
Quick version (raw JSON):
- docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ml.pred.alert.eta \
  --from-beginning
Pretty version:
- pip install kafka-python
- python live_monitor.py

13) Verification sequence (live deployment)
1. Start Kafka, Kind, Service, Flink      (steps 1-6)
2. Start prediction logger                (step 9)
3. Start MQTT bridge                      (step 11)
4. Start terminal monitor                 (step 12)
5. Watch for first predictions (should appear within seconds of machines sending status)
6. If predictions flow → start dashboard  (step 10)

Acceptance gates (shadow mode, rolling):
- ETA: MedAE ≤ ~60s; Hit@±5m ≥ 75%; Hit@±10m ≥ 80%
- Coverage: P90 88–93% overall and per machine
- P90 per-machine (n≥200) ≥ 84%
- Type: display only if conf ≥ 0.6
- Service p95 latency < 100 ms; error rate < 1%

Nudge rule (per-machine P90):
- If P90 coverage < 88% or > 95% for 3 consecutive days for a machine:
  - Increase/decrease its multiplier by 2–5% in artifacts (config map)
  - Redeploy service

Troubleshooting:
- Pod crashing? → kubectl -n ml logs deployment/alert-eta-service --tail=50
- No predictions? → Check Flink terminal for errors, verify curl http://$KIND_IP:30080/health
- Dashboard crash? → Check terminal for Python errors, restart streamlit
- MQTT bridge not receiving? → docker logs mqtt_to_ml_kafka -f, verify MQTT_BROKER IP
- Stale data? → Delete Kafka topics and recreate: ./create-topics.sh