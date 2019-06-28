#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

FORMULAS="star-neg linear-neg triangle-neg"
NEGATE="" # if formulas above are suffixed with -neg this should be "", otherwise "-negate"
EVENT_RATES="10000 15000 20000"
ACCELERATIONS="0 1"
INDEX_RATES="1 1000"
PROCESSORS="1/0-2,24-26 4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
LOG_LENGTH=60

echo $LOG_LENGTH > "$REPORT_DIR/gen.length"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Synthetic experiments (relation sizes) ==="

make_log() {
    flag=$1
    formula=$2
    for er in $EVENT_RATES; do
        for ir in $INDEX_RATES; do
            "$WORK_DIR/generator.sh" $flag -e $er -i $ir -w 10 -pA 0.01 -pB 0.495 $LOG_LENGTH > "$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
            "$WORK_DIR/replayer.sh" -a 0 -q $REPLAYER_QUEUE -i csv -f monpoly "$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv" > "$OUTPUT_DIR/gen_${formula}_${er}_${ir}.log"
        done
    done
}

echo "Generating logs ..."
make_log -S star-neg
make_log -L linear-neg
make_log -T triangle-neg


echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    for er in $EVENT_RATES; do
        for ir in $INDEX_RATES; do
        echo "    Event rate $er, index rate $ir:"
            for acc in $ACCELERATIONS; do
            echo "    Acceleration $acc:"
                for i in $(seq 1 $REPETITIONS); do
                    echo "      Repetition $i ..."

                    if [[ "$acc" = "0" ]]; then

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.log"
                        TIME_REPORT="$REPORT_DIR/gen_monpoly_${formula}_${er}_${ir}_${acc}_${i}_time.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        cat "$INPUT_FILE" | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/synthetic/synth.sig" -formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE > "$VERDICT_FILE"

                    else

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                        DELAY_REPORT="$REPORT_DIR/gen_monpoly_${formula}_${er}_${ir}_${acc}_${i}_delay.txt"
                        TIME_REPORT="$REPORT_DIR/gen_monpoly_${formula}_${er}_${ir}_${acc}_${i}_time.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f monpoly "$INPUT_FILE" 2> "$DELAY_REPORT") \
                            | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/synthetic/synth.sig" -formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE -nonewlastts > "$VERDICT_FILE"

                    fi


                done
            done
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
        for er in $EVENT_RATES; do
            for ir in $INDEX_RATES; do
            echo "      Event rate $er, index rate $ir:"
                for acc in $ACCELERATIONS; do
                echo "    Acceleration $acc:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."


                        if [[ "$acc" = "0" ]]; then

                            INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                            JOB_NAME="gen_flink_monpoly_${numcpus}_${formula}_${er}_${ir}_${acc}_${i}"
                            TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                            BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                            JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                            rm -r "$VERDICT_FILE" 2> /dev/null
                            "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                            wait

                        else

                            INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                            JOB_NAME="gen_flink_monpoly_${numcpus}_${formula}_${er}_${ir}_${acc}_${i}"
                            DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                            TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                            BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                            JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                            rm -r "$VERDICT_FILE" 2> /dev/null
                            taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                            "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                            wait

                        fi


                    done
                done
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "Flink with checkpointing:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        for er in $EVENT_RATES; do
            for ir in $INDEX_RATES; do
            echo "      Event rate $er, index rate $ir:"
                for acc in $ACCELERATIONS; do
                echo "    Acceleration $acc:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                        JOB_NAME="gen_flink_monpoly_ft_${numcpus}_${formula}_${er}_${ir}_${acc}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                        wait

                    done
                done
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "Flink with checkpointing and statistics:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        for er in $EVENT_RATES; do
            for ir in $INDEX_RATES; do
            echo "      Event rate $er, index rate $ir:"
                for acc in $ACCELERATIONS; do
                echo "    Acceleration $acc:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                        JOB_NAME="gen_flink_monpoly_ft_stats_${numcpus}_${formula}_${er}_${ir}_${acc}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a ${acc} -q $REPLAYER_QUEUE -i csv -f csv -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                        wait
                    done
                done
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
echo "Scraping metrics from $start_time to $end_time ..."
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time gen)

echo
echo "Evaluation complete!"
