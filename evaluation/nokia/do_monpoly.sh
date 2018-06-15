#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

ITERATIONS=1
FORMULAS="script1.mfotl del-1-2.mfotl"
ACCELERATION=3000
REPORT_FILE="$REPORT_DIR/nokia_monpoly.txt"

if [[ -n $1 ]]; then
    "$WORK_DIR/replayer.sh" -a $ACCELERATION -m "$WORK_DIR/ldcc_sample.csv" | "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$1" -negate > /dev/null
    exit $?
fi

if [[ -f $REPORT_FILE ]]; then
    echo "[ERROR]: The report file already exists."
    echo "Please delete $REPORT_FILE first."
    exit 1
fi

echo "=== NOKIA / MONPOLY ==========================================================="

for formula in $FORMULAS; do
    echo "Evaluating $formula:"
    echo "$formula" >> "$REPORT_FILE"
    for i in $(seq 1 $ITERATIONS); do
        echo "  Iteration $i ..."
        taskset -c $CPU_LIST $TIME_COMMAND -a -f "%e %M" -o "$REPORT_FILE" "$WORK_DIR/nokia/do_monpoly.sh" "$WORK_DIR/nokia/$formula"
    done
    echo >> "$REPORT_FILE"
done

echo "Evaluation complete!"
echo
