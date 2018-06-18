#!/usr/bin/env bash

set -e

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="script1 del-1-2"
ACCELERATIONS=3000

echo "=== NOKIA / MONPOLY ==========================================================="

for formula in $FORMULAS; do
    echo "Evaluating $formula:"

    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"

    echo "  Computing initial state ..."

    SAVE_COMMAND="$OUTPUT_DIR/save_state.tmp"
    echo ">save_state \"${STATE_FILE}\"<" > "$SAVE_COMMAND"
    "$WORK_DIR/replayer.sh" -a 0 -m "$WORK_DIR/ldcc_sample_past.csv" | cat - "$SAVE_COMMAND" \
        | "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -negate > /dev/null
    rm "$SAVE_COMMAND"

    for acc in $ACCELERATIONS; do
        echo "  acceleration $acc:"
        for i in $(seq 1 $REPETITIONS); do
            echo "    repetition $i ..."

            DELAY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_${i}_delay.txt"
            SUMMARY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_${i}_summary.txt"

            (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -m "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                | taskset -c $CPU_LIST "$TIME_COMMAND" -f "%e %M" -o "$SUMMARY_REPORT" "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -load "$STATE_FILE" -negate > /dev/null
        done
    done
done

echo "Evaluation complete!"
echo
