#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="script1 ins-1-2 del-1-2"
ACCELERATIONS="500 1000 1500 2000 2500 3000 4000 5000"
PROCESSORS="1/0-2,24-26 2/0-3,24-27 4/0-5,24-29"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
REPLAYER_QUEUE=1200

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

#echo "Monpoly standalone:"
#for formula in $FORMULAS; do
#    echo "  Evaluating $formula:"
#    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
#    for acc in $ACCELERATIONS; do
#        echo "    Acceleration $acc:"
#        for i in $(seq 1 $REPETITIONS); do
#            echo "      Repetition $i ..."
#
#            DELAY_REPORT="$REPORT_DIR/nokia2_monpoly_${formula}_${acc}_${i}_delay.txt"
#            TIME_REPORT="$REPORT_DIR/nokia2_monpoly_${formula}_${acc}_${i}_time.txt"
#
#            rm "$VERDICT_FILE" 2> /dev/null
#            (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -m "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
#                | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -load "$STATE_FILE" -negate > "$VERDICT_FILE"
#        done
#    done
#done

# # Stop visual monitors if running
# "$WORK_DIR/visual/stop.sh" > /dev/null
# # start visual monitors
# "$WORK_DIR/visual/start.sh" > /dev/null

start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

#echo "Flink with Monpoly, no checkpointing:"
#for procs in $PROCESSORS; do
#    numcpus=${procs%/*}
#    cpulist=${procs#*/}
#    echo "  $numcpus processors:"
#
#    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null
#
#    for formula in $FORMULAS; do
#        echo "    Evaluating $formula:"
#        STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
#        for acc in $ACCELERATIONS; do
#            echo "      Acceleration $acc:"
#            for i in $(seq 1 $REPETITIONS); do
#                echo "        Repetition $i ..."
#
#                JOB_NAME="nokia2_flink_${numcpus}_${formula}_${acc}_${i}"
#                DELAY_REPORT="$REPORT_DIR/nokia2_flink_${numcpus}_${formula}_${acc}_${i}_delay.txt"
#                TIME_REPORT="$REPORT_DIR/nokia2_flink_${numcpus}_${formula}_${acc}_${i}_time_{ID}.txt"
#                JOB_REPORT="$REPORT_DIR/nokia2_flink_${numcpus}_${formula}_${acc}_${i}_job.txt"
#
#                rm "$VERDICT_FILE" 2> /dev/null
#                taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
#                "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate -load $STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
#                wait
#            done
#        done
#    done
#
#    "$FLINK_BIN/stop-cluster.sh" > /dev/null
#done

echo "Flink with Monpoly, checkpointing:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."

                JOB_NAME="nokia2_flink_ft_${numcpus}_${formula}_${acc}_${i}"
                DELAY_REPORT="$REPORT_DIR/nokia2_flink_ft_${numcpus}_${formula}_${acc}_${i}_delay.txt"
                TIME_REPORT="$REPORT_DIR/nokia2_flink_ft_${numcpus}_${formula}_${acc}_${i}_time_{ID}.txt"
                JOB_REPORT="$REPORT_DIR/nokia2_flink_ft_${numcpus}_${formula}_${acc}_${i}_job.txt"

                rm "$VERDICT_FILE" 2> /dev/null
                taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate -load $STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                wait
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
#echo "Scraping metrics..."
#(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time nokia)
echo "Metrics period: $start_time $end_time"

echo
echo "Evaluation complete!"
