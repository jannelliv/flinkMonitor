#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="script1-past-ltl"
ACCELERATIONS="0 500 1000 1500 2000 2500 3000 3500 4000"
PROCESSORS="1/0-2,24-26 2/0-3,24-27 4/0-5,24-29"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
REPLAYER_QUEUE=10000

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Nokia experiments cmp w/ Dejavu ==="


echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    for acc in $ACCELERATIONS; do
        echo "    Acceleration $acc:"
        for i in $(seq 1 $REPETITIONS); do
            echo "      Repetition $i ..."

            DELAY_REPORT="$REPORT_DIR/nokiaCMP_monpoly_${formula}_${acc}_${i}_delay.txt"
            TIME_REPORT="$REPORT_DIR/nokiaCMP_monpoly_${formula}_${acc}_${i}_time.txt"

            rm -r "$VERDICT_FILE" 2> /dev/null
            (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f monpoly-linear "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" -negate > "$VERDICT_FILE"
        done
    done
done

echo "Dejavu standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    for acc in $ACCELERATIONS; do
        echo "    Acceleration $acc:"
        for i in $(seq 1 $REPETITIONS); do
            echo "      Repetition $i ..."

            DELAY_REPORT="$REPORT_DIR/nokiaCMP_dejavu_${formula}_${acc}_${i}_delay.txt"
            TIME_REPORT="$REPORT_DIR/nokiaCMP_dejavu_${formula}_${acc}_${i}_time.txt"

            rm -r "$VERDICT_FILE" 2> /dev/null
            (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f dejavu-linear "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$DEJAVU_EXE" "$WORK_DIR/nokia/$formula.mfotl.neg.qtl"  20 "print" > "$VERDICT_FILE"
        done
    done
done


start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo "Flink without checkpointing:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."

                JOB_NAME="nokiaCMP_flink_monpoly_${numcpus}_${formula}_${acc}_${i}"
                DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f monpoly-linear -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format monpoly --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -negate" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                wait

                JOB_NAME="nokiaCMP_flink_dejavu_${numcpus}_${formula}_${acc}_${i}"
                DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f dejavu-linear -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format dejavu --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                wait
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done


end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
echo "Scraping metrics from $start_time to $end_time ..."
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time nokia)

echo
echo "Evaluation complete!"
