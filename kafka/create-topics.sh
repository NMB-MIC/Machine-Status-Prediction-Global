#!/usr/bin/env bash
set -e

BROKER=${1:-localhost:9092}

for t in iot.machine.status.raw ml.pred.alert.eta iot.machine.status.dlq; do
  docker exec -it kafka kafka-topics.sh \
    --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic $t --replication-factor 1 --partitions 8
done

docker exec -it kafka kafka-topics.sh --bootstrap-server $BROKER --list