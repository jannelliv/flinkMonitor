#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

ITERATIONS=3
FORMULAS="script1.mfotl del-1-2.mfotl"
REPORT_FILE="$REPORT_DIR/nokia_monpoly.txt"

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
        taskset -c $CPU_LIST $TIME_COMMAND -a -f "%e %M" -o "$REPORT_FILE" "$MONPOLY" -sig "$LDCC_SIG" -formula "$WORK_DIR/nokia/$formula" -negate -log "$LDCC_SAMPLE" > /dev/null
    done
    echo >> "$REPORT_FILE"
done

echo "Evaluation complete!"
echo
