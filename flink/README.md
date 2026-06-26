Prereqs:
- Flink 1.17+ installed; ensure flink-connector-kafka-1.17.x.jar is in $FLINK_HOME/lib
  (Download from: https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/)
- Kafka running on localhost:9092
- Model service deployed in K8s (Service DNS: alert-eta-service.ml.svc.cluster.local:8080)

Run:
- python job.py