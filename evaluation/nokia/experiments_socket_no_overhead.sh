#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

FORMULAS="ins-1-2-neg"
#FORMULAS="custom-neg ins-1-2-neg del-1-2-neg"
NEGATE=""
#MULTISOURCE_VARIANTS="2 4"
#ACCELERATIONS="1000 2000 3000 4000 5000"
#PROCESSORS="1/0-2,24-26 2/0-3,24-27 4/0-5,24-29 8/0-9,24-33"
MULTISOURCE_VARIANTS="2"
ACCELERATIONS="1000 3000 5000 6000"
PROCESSORS="8"
KAFKA_PARTS="1"
MONPOLY_CPU_LIST="0"
MONPOLY_CMDS="$MONPOLY_EXE"
AUX_CPU_LIST="10-11,34-35"

cat "$ROOT_DIR/ldcc_sample.csv" | wc -l > "$REPORT_DIR/nokia.events"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Nokia experiments ==="

echo "Creating a monpoly version of the log ..."

if [[ -a "$ROOT_DIR/ldcc_sample.log" ]]; then
    echo "monpoly version already exists, skipping"
else
    "$WORK_DIR/replayer.sh" -a 0 -q "$REPLAYER_QUEUE" -i csv -f monpoly "$ROOT_DIR/ldcc_sample.csv" > "$ROOT_DIR/ldcc_sample.log"
fi



for formula in $FORMULAS; do
    echo "Computing initial state for $formula ..."

    STATE_FILE="$STATE_DIR/ldcc_sample_past_${formula}.state"

    if [[ -a "$STATE_FILE" ]]; then
        echo "state $STATE_FILE already exists, skipping"
    else
        SAVE_COMMAND="$OUTPUT_DIR/save_state.tmp"
        echo ">save_state \"${STATE_FILE}\"<" > "$SAVE_COMMAND"
        "$WORK_DIR/replayer.sh" -a 0 -i csv -f monpoly "$ROOT_DIR/ldcc_sample_past.csv" | cat - "$SAVE_COMMAND" \
            | "$MONPOLY_EXE" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" $NEGATE > /dev/null
        rm "$SAVE_COMMAND"

    fi
done

: '
echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    STATE_FILE="$STATE_DIR/ldcc_sample_past_${formula}.state"
    for acc in $ACCELERATIONS; do
        echo "    Acceleration $acc:"
        for i in $(seq 1 "$REPETITIONS"); do
            echo "      Repetition $i ..."

            if [[ "$acc" = "0" ]]; then

                TIME_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_0_${i}_time.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                cat "$ROOT_DIR/ldcc_sample.log" | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -load "$STATE_FILE" $NEGATE > "$VERDICT_FILE"

            else

                DELAY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_1_${i}_delay.txt"
                TIME_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_1_${i}_time.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f monpoly "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                    | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -load "$STATE_FILE" $NEGATE -nonewlastts > "$VERDICT_FILE"

            fi


        done
    done
done
'

start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

#"$ZOOKEEPER_EXE" start &> /dev/null || fail "failed to start zookeeper"
#sleep 3.0
#"$KAFKA_BIN/kafka-server-start.sh" -daemon "$KAFKA_CONFIG_FILE" &> /dev/null &
"$FLINK_BIN/start-cluster.sh" &> /dev/null || fail "failed to start flink"
echo "Flink without checkpointing:"
for numsources in $KAFKA_PARTS; do
    echo "$numsources kafka part:"
    for procs in $PROCESSORS; do
        echo "  $procs processors:"
        for variant in $MULTISOURCE_VARIANTS; do
            echo "      Variant $variant:"
            rm -rf "$EXEC_LOG_DIR/preprocess_out"*
            "$WORK_DIR"/trace-transformer.sh -v $variant -n $numsources -o "$EXEC_LOG_DIR/preprocess_out" "$ROOT_DIR/ldcc_sample.csv"
            for formula in $FORMULAS; do
                echo "          Evaluating $formula:"
                STATE_FILE="$STATE_DIR/ldcc_sample_past_${formula}.state"
                for acc in $ACCELERATIONS; do
                    echo "              Acceleration $acc:"
                    for cmd in $MONPOLY_CMDS; do
                        cmd_string=$(monpoly_cmd_to_string $cmd)
                        echo "                  Monpoly cmd: ${cmd_string}"
                        for i in $(seq 1 $REPETITIONS); do
                            echo "                      Repetition $i ..."
                            JOB_NAME="nokia_flink_monpoly_${numsources}_${procs}_${cmd_string}_${variant}_${formula}_${acc}_1_${i}"
                            DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                            TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                            BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                            JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                            rm -r "$VERDICT_FILE" 2> /dev/null
                            "$WORK_DIR/replayer.sh" --other_branch -o "127.0.0.1:6060" -v $(variant_replayer_params $variant) -n $numsources -a $acc -q $REPLAYER_QUEUE -i csv -f csv -t 1000 "$EXEC_LOG_DIR/preprocess_out" 2> "$DELAY_REPORT" &
                            "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "127.0.0.1:6060" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $cmd -nonewlastts $NEGATE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $procs --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" --multi $variant --clear false --nparts $numsources $(monpoly_cmd_to_flink_args "$cmd" "$STATE_FILE") > "$JOB_REPORT"
                            wait
                        done # reps
                    done # cmd
                done # acc
            done # formula
        done # variant
    done #procs
done #numsources
#"$FLINK_BIN/stop-cluster.sh" &> /dev/null || fail "failed to stop flink"
#"$KAFKA_BIN/kafka-server-stop.sh" &> /dev/null || fail "failed to stop kafka"
#sleep 1.0
#"$ZOOKEEPER_EXE" stop &> /dev/null || fail "failed to stop zookeeper"


: '
echo "Flink with checkpointing:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        STATE_FILE="$STATE_DIR/ldcc_sample_past_${formula}.state"
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."

                if [[ "$acc" = "0" ]]; then

                    JOB_NAME="nokia_flink_monpoly_ft_${numcpus}_${formula}_${acc}_0_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    mkdir -p "$ROOT_DIR/input"
                    ln -s "$ROOT_DIR/ldcc_sample.csv" "$ROOT_DIR/input/ldcc_sample.csv"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --watch true --in "$ROOT_DIR/input" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --load "$STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait

                    rm -r "$ROOT_DIR/input/*"

                else

                    JOB_NAME="nokia_flink_monpoly_ft_${numcpus}_${formula}_${acc}_1_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --load "$STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait

                fi
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "Flink without checkpointing with statistics:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        STATE_FILE="$STATE_DIR/ldcc_sample_past_${formula}.state"
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."


                if [[ "$acc" = "0" ]]; then

                    JOB_NAME="nokia_flink_monpoly_stats_${numcpus}_${formula}_${acc}_0_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    HEAVY_FILE="$WORK_DIR/nokia/stats_nokia.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$ROOT_DIR/ldcc_sample.csv" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --load "$STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                else

                    JOB_NAME="nokia_flink_monpoly_stats_${numcpus}_${formula}_${acc}_1_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    HEAVY_FILE="$WORK_DIR/nokia/stats_nokia.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --load "$STATE_FILE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait

                fi


            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done
'
end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
echo "Scraping metrics from $start_time to $end_time ..."
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time nokia)

echo
echo "Evaluation complete!"
