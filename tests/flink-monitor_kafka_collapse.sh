#!/bin/bash

PROCESSORS=4

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH="$WORKDIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
DATADIR="$WORKDIR/evaluation/synthetic"

if [[ ! -r $JARPATH ]]; then
    echo "Error: Could not find monitor jar: $JARPATH"
    exit 2
fi

FLINKDIR="$1"
if [[ -z $FLINKDIR || ! -x $FLINKDIR/bin/flink ]]; then
    echo "Usage: $0 <path to Flink>"
    exit 2
fi

TEMPDIR="$(mktemp -d)"
#trap 'rm -rf "$TEMPDIR"' EXIT
echo "$TEMPDIR"

fail() {
    echo "=== Test failed ==="
    exit 1
}

echo "Generating log ..."
"$WORKDIR/generator.sh" -T -e 1000 -i 100 -x 1 60 > "$TEMPDIR/trace.csv" && \
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
"$FLINKDIR/bin/flink" run "$JARPATH" --multi 1 --in "kafka" --kafkatestfile "$TEMPDIR/trace.csv" --format csv --sig "$DATADIR/synth.sig" --formula "$DATADIR/triangle-neg.mfotl" \
        --negate false --monitor monpoly --command "/home/rofl/git/scalable-online-monitor/evaluation/run-monpoly.sh {ID}" --processors $PROCESSORS --out "$TEMPDIR/flink-out"
if [[ $? != 0 ]]; then
    fail
fi
find "$TEMPDIR/flink-out" -type f -exec cat \{\} + > "$TEMPDIR/out.txt"

echo
if "$WORKDIR/tests/verdicts_diff.py" -c "$TEMPDIR/reference.txt" "$TEMPDIR/out.txt"; then
    echo "=== Test passed ==="
else
    fail
fi

