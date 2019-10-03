#!/bin/bash

PROCESSORS=1
KAFKA_SERVER="localhost:9092"
TOPIC="flink-monitor-test"

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH="$WORKDIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
DATADIR="$WORKDIR/evaluation/synthetic"

if [[ ! -r $JARPATH ]]; then
    echo "Error: Could not find monitor jar: $JARPATH"
    exit 2
fi

FLINKDIR="$1"
if [[ -z $FLINKDIR || ! -x $FLINKDIR/bin/flink ]]; then
    echo "Usage: $0 <path to Flink> <path to Kafka>"
    exit 2
fi

KAFKADIR="$2"
if [[ -z $KAFKADIR || ! -x $KAFKADIR/bin/kafka-topics.sh ]]; then
    echo "Usage: $0 <path to Flink> <path to Kafka>"
    exit 2
fi

"$KAFKADIR/bin/kafka-topics.sh" --delete --bootstrap-server "$KAFKA_SERVER" --topic "$TOPIC" > /dev/null 2> /dev/null
"$KAFKADIR/bin/kafka-topics.sh" --create --bootstrap-server "$KAFKA_SERVER" --topic "$TOPIC" --replication-factor 1 --partitions 1


TEMPDIR="$(mktemp -d)"
trap 'rm -rf "$TEMPDIR"' EXIT

fail() {
    echo "=== Test failed ==="
    exit 1
}

echo "Generating log ..."
"$WORKDIR/generator.sh" -T -e 2000 -i 10 -x 50 60 > "$TEMPDIR/trace.csv" && \
        "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 "$TEMPDIR/trace.csv" > "$TEMPDIR/trace.log"
if [[ $? != 0 ]]; then
    fail
fi

echo "Creating reference output ..."
monpoly -sig "$DATADIR/synth.sig" -formula "$DATADIR/triangle-neg.mfotl" -log "$TEMPDIR/trace.log" > "$TEMPDIR/reference.txt"
if [[ $? != 0 ]]; then
    fail
fi

echo "Running Flink monitor ..."
printf '>end<\n' >> "$TEMPDIR/trace.csv"
"$KAFKADIR/bin/kafka-console-producer.sh" --broker-list "$KAFKA_SERVER" --topic "$TOPIC" < "$TEMPDIR/trace.csv" > /dev/null
mkdir -p "$TEMPDIR/checkpoints"
"$FLINKDIR/bin/flink" run "$JARPATH" --in "$KAFKA_SERVER" --kafka-topic "$TOPIC" --kafka-group "$TOPIC" --format csv \
        --sig "$DATADIR/synth.sig" --formula "$DATADIR/triangle-neg.mfotl" \
        --negate false --monitor monpoly --processors $PROCESSORS --out "$TEMPDIR/flink-out" \
        --checkpoints "file://$TEMPDIR/checkpoints" --checkpoint-interval 2000 --restarts 1 --inject-fault 61000 --queueSize 500
if [[ $? != 0 ]]; then
    fail
fi
find "$TEMPDIR/flink-out" -type f -exec cat \{\} + > "$TEMPDIR/out.txt"

echo
if "$WORKDIR/tests/verdicts_diff.py" "$TEMPDIR/reference.txt" "$TEMPDIR/out.txt"; then
    echo "=== Test passed ==="
else
    fail
fi
