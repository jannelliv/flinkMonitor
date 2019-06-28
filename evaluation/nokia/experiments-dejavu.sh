#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

FORMULAS="custom-neg"
NEGATE=""
NEGATE_DEJAVU="" #"--negate false"
ACCELERATIONS="500 1000 1500 2000"
PROCESSORS="1/0-2,24-26 2/0-3,24-27 4/0-5,24-29 8/0-9,24-33"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"

cat "$ROOT_DIR/ldcc_sample_linear.csv" | wc -l > "$REPORT_DIR/nokiaCMP.events"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Nokia experiments cmp w/ Dejavu ==="

echo "Creating a linear version of the log ..."

if [[ -a "$ROOT_DIR/ldcc_sample_linear.csv" ]]; then
    echo "linear version already exists, skipping"
else
    "$WORK_DIR/replayer.sh" -a 0 -q $REPLAYER_QUEUE -i csv -f csv-linear "$ROOT_DIR/ldcc_sample.csv" > "$ROOT_DIR/ldcc_sample_linear.csv"
fi

echo "Creating a linear monpoly version of the log ..."

if [[ -a "$ROOT_DIR/ldcc_sample_linear.log" ]]; then
    echo "linear monpoly version already exists, skipping"
else
    "$WORK_DIR/replayer.sh" -a 0 -q $REPLAYER_QUEUE -i csv -f monpoly-linear "$ROOT_DIR/ldcc_sample.csv" > "$ROOT_DIR/ldcc_sample_linear.log"
fi

echo "Creating a linear dejavu version of the log ..."

if [[ -a "$ROOT_DIR/ldcc_sample_linear.dvu" ]]; then
    echo "linear dejavu version already exists, skipping"
else
    "$WORK_DIR/replayer.sh" -a 0 -q $REPLAYER_QUEUE -i csv -f dejavu-linear "$ROOT_DIR/ldcc_sample.csv" > "$ROOT_DIR/ldcc_sample_linear.dvu"
fi


echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    for acc in $ACCELERATIONS; do
        echo "    Acceleration $acc:"
        for i in $(seq 1 $REPETITIONS); do
            echo "      Repetition $i ..."

            if [[ "$acc" = "0" ]]; then

                TIME_REPORT="$REPORT_DIR/nokiaCMP_monpoly_${formula}_${acc}_0_${i}_time.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                cat "$ROOT_DIR/ldcc_sample_linear.log" | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" $NEGATE > "$VERDICT_FILE"


            else

                DELAY_REPORT="$REPORT_DIR/nokiaCMP_monpoly_${formula}_${acc}_1_${i}_delay.txt"
                TIME_REPORT="$REPORT_DIR/nokiaCMP_monpoly_${formula}_${acc}_1_${i}_time.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f monpoly-linear "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                    | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/nokia/ldcc.sig" -formula "$WORK_DIR/nokia/$formula.mfotl" $NEGATE -nonewlastts > "$VERDICT_FILE"

            fi



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

            fma=$("$DEJAVU_EXE" "compile" "$WORK_DIR/nokia/$formula.mfotl.neg.qtl")

            if [[ "$acc" = "0" ]]; then

                TIME_REPORT="$REPORT_DIR/nokiaCMP_dejavu_${formula}_${acc}_0_${i}_time.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                cat "$ROOT_DIR/ldcc_sample_linear.dvu" | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$DEJAVU_EXE" "run" "$fma" 25 > "$VERDICT_FILE"

            else

                DELAY_REPORT="$REPORT_DIR/nokiaCMP_dejavu_${formula}_${acc}_1_${i}_delay.txt"
                TIME_REPORT="$REPORT_DIR/nokiaCMP_dejavu_${formula}_${acc}_1_${i}_time.txt"

                rm -r "$VERDICT_FILE" 2> /dev/null
                (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f dejavu-linear "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT") \
                    | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$DEJAVU_EXE" "run" "$fma" 25 > "$VERDICT_FILE"

            fi


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

                if [[ "$acc" = "0" ]]; then

                    echo "          Monpoly ..."
                    JOB_NAME="nokiaCMP_flink_monpoly_${numcpus}_${formula}_${acc}_0_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$ROOT_DIR/ldcc_sample_linear.csv" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                    echo "          Dejavu ..."
                    JOB_NAME="nokiaCMP_flink_dejavu_${numcpus}_${formula}_${acc}_0_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$ROOT_DIR/ldcc_sample_linear.csv" --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" $NEGATE_DEJAVU --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                else

                    echo "          Monpoly ..."
                    JOB_NAME="nokiaCMP_flink_monpoly_${numcpus}_${formula}_${acc}_1_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                    echo "          Dejavu ..."
                    JOB_NAME="nokiaCMP_flink_dejavu_${numcpus}_${formula}_${acc}_1_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" $NEGATE_DEJAVU --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
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
        for acc in $ACCELERATIONS; do
            echo "      Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "        Repetition $i ..."

                if [[ "$acc" = "0" ]]; then

                    echo "          Monpoly ..."
                    JOB_NAME="nokiaCMP_flink_monpoly_stats_${numcpus}_${formula}_${acc}_0_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    HEAVY_FILE="$WORK_DIR/nokia/stats_nokia.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$ROOT_DIR/ldcc_sample_linear.csv" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                    echo "          Dejavu ..."
                    JOB_NAME="nokiaCMP_flink_dejavu_stats_${numcpus}_${formula}_${acc}_0_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    HEAVY_FILE="$WORK_DIR/nokia/stats_nokia.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$ROOT_DIR/ldcc_sample_linear.csv" --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" $NEGATE_DEJAVU --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                else

                    echo "          Monpoly ..."
                    JOB_NAME="nokiaCMP_flink_monpoly_stats_${numcpus}_${formula}_${acc}_1_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    HEAVY_FILE="$WORK_DIR/nokia/stats_nokia.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                    echo "          Dejavu ..."
                    JOB_NAME="nokiaCMP_flink_dejavu_stats_${numcpus}_${formula}_${acc}_1_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                    HEAVY_FILE="$WORK_DIR/nokia/stats_nokia.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$ROOT_DIR/ldcc_sample.csv" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" $NEGATE_DEJAVU --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $numcpus --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                fi



            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done


end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
echo "Scraping metrics from $start_time to $end_time ..."
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time nokiaCMP)

echo
echo "Evaluation complete!"
