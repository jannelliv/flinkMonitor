#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="script1 ins-1-2 del-1-2"
ACCELERATIONS="500 1000 1500 2000 2500 3000 4000 5000"
PROCESSORS="1/0-1,24-25 4/0-4,24-28 8/0-8,24-32 16/0-8,12-19,24-32,36-43"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="9-10,33-34"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

for formula in $FORMULAS; do
    echo "Computing initial state for $formula ..."

    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
    SAVE_COMMAND="$OUTPUT_DIR/save_state.tmp"
    echo ">save_state \"${STATE_FILE}\"<" > "$SAVE_COMMAND"
    "$WORK_DIR/replayer.sh" -a 0 -m "$WORK_DIR/ldcc_sample_past.csv" | cat - "$SAVE_COMMAND" \
        | "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -negate > /dev/null
    rm "$SAVE_COMMAND"
done

echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
    for acc in $ACCELERATIONS; do
        echo "    Acceleration $acc:"
        for i in $(seq 1 $REPETITIONS); do
            echo "      Repetition $i ..."

            DELAY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_${i}_delay.txt"
            SUMMARY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_${i}_summary.txt"

            rm "$VERDICT_FILE"
            (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -m "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e %M" -o "$SUMMARY_REPORT" "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -load "$STATE_FILE" -negate > "$VERDICT_FILE"
        done
    done
done

echo "Flink with Monpoly, no checkpointing:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null
    "$WORK_DIR/visual/start.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."

                JOB_NAME="nokia_flink_${numcpus}_${formula}_${acc}_${i}"
                DELAY_REPORT="$REPORT_DIR/nokia_flink_${numcpus}_${formula}_${acc}_${i}_delay.txt"

                rm "$VERDICT_FILE"
                taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -o localhost:$STREAM_PORT "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$WORK_DIR/monpoly -negate -load $STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME"
            done
        done
    done

    "$WORK_DIR/visual/stop.sh" > /dev/null
    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "Flink with Monpoly, checkpointing:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null
    "$WORK_DIR/visual/start.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."

                JOB_NAME="nokia_flink_ft_${numcpus}_${formula}_${acc}_${i}"
                DELAY_REPORT="$REPORT_DIR/nokia_flink_ft_${numcpus}_${formula}_${acc}_${i}_delay.txt"

                rm "$VERDICT_FILE"
                taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -o localhost:$STREAM_PORT "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$WORK_DIR/monpoly -negate -load $STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME"
            done
        done
    done

    "$WORK_DIR/visual/stop.sh" > /dev/null
    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo
echo "Evaluation complete!"
