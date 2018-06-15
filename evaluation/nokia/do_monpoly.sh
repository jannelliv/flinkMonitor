#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

ITERATIONS=1
FORMULAS="script1.mfotl del-1-2.mfotl"
ACCELERATION=3000

REPORT_FILE1="$REPORT_DIR/nokia_monpoly1.txt"
REPORT_FILE2="$REPORT_DIR/nokia_monpoly2.txt"

if [[ -f $REPORT_FILE1 || -f $REPORT_FILE2 ]]; then
    echo "[ERROR]: The report files already exist. Please delete them first:"
    echo "  $REPORT_FILE1"
    echo "  $REPORT_FILE2"
    exit 1
fi

echo "=== NOKIA / MONPOLY ==========================================================="

for formula in $FORMULAS; do
    echo "Evaluating $formula:"
    echo "$formula" >> "$REPORT_FILE1"
    echo "$formula" >> "$REPORT_FILE2"
    for i in $(seq 1 $ITERATIONS); do
        echo "  Iteration $i ..."
        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -a $ACCELERATION -m "$WORK_DIR/ldcc_sample.csv" 2>> "$REPOT_FILE1" \
            | taskset -c $CPU_LIST $TIME_COMMAND -a -f "%e %M" -o "$REPORT_FILE2" "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$1" -negate > /dev/null
    done
    echo >> "$REPORT_FILE1"
    echo >> "$REPORT_FILE2"
done

echo "Evaluation complete!"
echo
