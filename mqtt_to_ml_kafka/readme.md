## ML MQTT-to-Kafka Bridge

Subscribes to machine status events from MQTT and produces to our ML Kafka topic.

### Flow

### Build & Run
docker build --no-cache -t mic/mqtt_ml_kafka:1.0.0 .
docker compose up -d

### Verify
docker logs mqtt_to_ml_kafka -f


