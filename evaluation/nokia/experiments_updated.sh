#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

format="+%Y-%m-%dT%H:%M:%S.%3NZ"

REPETITIONS=1
FORMULAS="ins-1-2 script1"
ACCELERATIONS="3000"
PROCESSORS="2/0-3,24-27 4/0-5,24-29 8/0-9,24-33"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
WINDOWS="2 4"
STATS="predictive reactive"
REPLAYER_QUEUE=1200

echo "=== Nokia experiments ==="

for formula in $FORMULAS; do
    echo "Computing initial state for $formula ..."

    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
    SAVE_COMMAND="$OUTPUT_DIR/save_state.tmp"
    echo ">save_state ${STATE_FILE}<" > "$SAVE_COMMAND"
    "$WORK_DIR/replayer.sh" -a 0 -m "$WORK_DIR/ldcc_sample_past.csv" | cat - "$SAVE_COMMAND" \
        | "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -negate > /dev/null
    rm "$SAVE_COMMAND"
done

echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"

    VERDICT_FILE="$OUTPUT_DIR/verdicts_${formula}.txt"
    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"

    for acc in $ACCELERATIONS; do
        echo "    Acceleration $acc:"
        for i in $(seq 1 $REPETITIONS); do
            echo "      Repetition $i ..."

            DELAY_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_${i}_delay.txt"
            TIME_REPORT="$REPORT_DIR/nokia_monpoly_${formula}_${acc}_${i}_time.txt"

            rm "$VERDICT_FILE" 2> /dev/null
            taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -m "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" \
                | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$WORK_DIR/monpoly" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -load $STATE_FILE -negate > "$VERDICT_FILE"
        done
    done
done

###Reshuffled part

for formula in $FORMULAS; do
    echo "Evaluating $formula:"

    STATE_FILE="$OUTPUT_DIR/ldcc_sample_past_${formula}.state"
    start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

    echo "Flink with adaptivity:"
    for procs in $PROCESSORS; do
        numcpus=${procs%/*}
        cpulist=${procs#*/}
        echo "  $numcpus processors:"

        BASE_DIR="$WORK_DIR/traces/$formula/degrees/$numcpus/windows"

        taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

        for window in $WINDOWS; do
            TRACE_DIR="$BASE_DIR/$window"
            echo "   $window windows":

            for stat in $STATS; do
                echo "    $stat statistics":
                TRACE_FILE="$TRACE_DIR/log-trace-$stat.csv"

                if [[ ! (-d "$TRACE_DIR") || ! (-f "$TRACE_FILE") ]]; then
                    mkdir -p $TRACE_DIR
                    GEN_REPORT="$REPORT_DIR/${JOB_NAME}_trace_gen.txt"
                    echo "   Generating trace files for: f=${formula}, p=${numcpus}, w=${window}"
                    taskset -c $AUX_CPU_LIST  "$WORK_DIR/monitor.sh" --formula $formula --inputDir $WORK_DIR --outputDir $TRACE_DIR --windows $window --parallelism $numcpus --analysis true &> $GEN_REPORT
                    #if ! "$WORK_DIR/monitor.sh" --formula $formula --inputDir $WORK_DIR --outputDir $TRACE_DIR --windows $window --parallelism $numcpus --analysis true; then
                    #    echo "Trace files could not be generated"
                    #    exit -1
                    #fi
                    echo "   Trace files generated"
                fi
                for acc in $ACCELERATIONS; do
                    echo "      Acceleration $acc:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."

                        JOB_NAME="nokia_flink_ft_${numcpus}_${formula}_${window}_${stat}_${acc}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        PROXY_LOG="$REPORT_DIR/${JOB_NAME}_proxy.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm "$VERDICT_FILE" 2> /dev/null
                        start_exp=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$TRACE_FILE" 2> "$DELAY_REPORT" &
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/proxy.sh" -i localhost:$STREAM_PORT -o $PROXY_PORT 2> "$PROXY_LOG" &
                        "$WORK_DIR/monitor.sh" --format csv --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$PROXY_PORT --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate -load $STATE_FILE -nofilteremptytp" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                        wait
                        end_exp=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

                        start_utc=$(echo "$start_exp" | xargs -I J date "$format" --utc -d J)
                        end_utc=$(echo "$end_exp" | xargs -I J date "$format" --utc -d J)

                        start_s=$(echo "$start_exp" | xargs -I J date '+%s'  -d J)
                        end_s=$(echo "$end_exp" | xargs -I J date '+%s'  -d J)

                        diff_s=$((end_s-start_s))
                        echo "        Experiment ran for $diff_s ms"
                    done
                done
            done
        done

        "$FLINK_BIN/stop-cluster.sh" > /dev/null
    done

    echo "Flink without adaptivity:"
    for procs in $PROCESSORS; do
        numcpus=${procs%/*}
        cpulist=${procs#*/}
        echo "  $numcpus processors:"

        taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

        for window in $WINDOWS; do
            echo "   $window windows":

            for stat in $STATS; do
             echo "    $stat statistics":
                for acc in $ACCELERATIONS; do
                    echo "      Acceleration $acc:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."
                        JOB_NAME="nokia_flink_${numcpus}_${formula}_${window}_${stat}_${acc}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        PROXY_LOG="$REPORT_DIR/${JOB_NAME}_proxy.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm "$VERDICT_FILE" 2> /dev/null
                        start_exp=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$WORK_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/proxy.sh" -i localhost:$STREAM_PORT -o $PROXY_PORT 2> "$PROXY_LOG" &
                        "$WORK_DIR/monitor.sh" --format csv --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$PROXY_PORT --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate -load $STATE_FILE -nofilteremptytp" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                        wait
                        end_exp=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

                        start_utc=$(echo "$start_exp" | xargs -I J date "$format" --utc -d J)
                        end_utc=$(echo "$end_exp" | xargs -I J date "$format" --utc -d J)

                        start_s=$(echo "$start_exp" | xargs -I J date '+%s'  -d J)
                        end_s=$(echo "$end_exp" | xargs -I J date '+%s'  -d J)

                        diff_s=$((end_s-start_s))
                        echo "        Experiment ran for $diff_s ms"
                    done
                done
            done
        done

        taskset -c $cpulist "$FLINK_BIN/stop-cluster.sh" > /dev/null
    done

    end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

    echo
    echo "Scraping metrics from $start_time to $end_time ..."
    (cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time "nokia-${formula}")

    echo "Stopping Flink"
    "$FLINK_BIN/stop-cluster.sh" > /dev/null

    echo
    echo "Evaluation complete for formula: ${formula}!"
done

###

exit 1