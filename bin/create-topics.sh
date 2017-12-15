#!/bin/bash

set -x

NUM_INPUT_PARTITIONS="$1"
[ -z "$NUM_INPUT_PARTITIONS" ] && NUM_INPUT_PARTITIONS=2

kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions $NUM_INPUT_PARTITIONS \
    --topic input-topic

### To create output topic with compaction enabled:
kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic output-topic \
    --config cleanup.policy=compact
