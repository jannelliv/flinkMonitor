#!/usr/bin/env bash

set -e

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

ITERATIONS=1
FORMULAS="script1 del-1-2"
ACCELERATION=3000

echo "=== NOKIA / MONPOLY ==========================================================="

for formula in $FORMULAS; do
    echo "Evaluating $formula:"
    for i in $(seq 1 $ITERATIONS); do
        echo "  Iteration $i ..."

        DELAY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${i}_delay.txt"
        SUMMARY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${i}_summary.txt"

        (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $ACCELERATION -m "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
            | taskset -c $CPU_LIST "$TIME_COMMAND" -f "%e %M" -o "$SUMMARY_REPORT" "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -negate > /dev/null
    done
done

echo "Evaluation complete!"
echo
