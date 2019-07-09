#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

FORMULAS="linear-past-ltl-neg triangle-past-ltl-neg"
#TODO: remember to change dejavu formulas for standalone (to remove the negation)
NEGATE="" # if formulas above are suffixed with -neg this should be "", otherwise "-negate"
NEGATE_DEJAVU=""  #"--negate false" # if formulas above are suffixed with -neg this should be "--negate true", otherwise ""
EVENT_RATES="50000 60000"
ACCELERATIONS="0 1"
#INDEX_RATES="1 1000" #do not exist for dejavu
PROCESSORS="8/0-9,24-33 16/0-8,12-20,24-32,36-44"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
LOG_LENGTH=60

echo $LOG_LENGTH > "$REPORT_DIR/genCMP.length"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Synthetic experiments (relation sizes) ==="

make_log() {
    flag=$1
    formula=$2
    for er in $EVENT_RATES; do
        "$WORK_DIR/generator.sh" $flag -e $er -i $er -w 10 -pA 0.01 -pB 0.495 $LOG_LENGTH > "$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"

        "$WORK_DIR/replayer.sh" -a 0 -q $REPLAYER_QUEUE -i csv -f monpoly-linear "$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv" > "$OUTPUT_DIR/gen_${formula}_${er}_${er}.log"

        "$WORK_DIR/replayer.sh" -a 0 -q $REPLAYER_QUEUE -i csv -f dejavu-linear "$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv" > "$OUTPUT_DIR/gen_${formula}_${er}_${er}.dvu"
    done
}

echo "Generating logs ..."
make_log -S star-past-ltl-neg
make_log -L linear-past-ltl-neg
make_log -T triangle-past-ltl-neg


echo "Monpoly standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    for er in $EVENT_RATES; do
        echo "    Event rate $er, index rate $er:"
        for acc in $ACCELERATIONS; do
            echo "    Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "      Repetition $i ..."

                if [[ "$acc" = "0" ]]; then

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.log"
                    TIME_REPORT="$REPORT_DIR/genCMP_monpoly_${formula}_${er}_${er}_${acc}_${i}_time.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    cat "$INPUT_FILE" | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/synthetic/synth.sig" -formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE > "$VERDICT_FILE"


                else

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                    DELAY_REPORT="$REPORT_DIR/genCMP_monpoly_${formula}_${er}_${er}_${acc}_${i}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/genCMP_monpoly_${formula}_${er}_${er}_${acc}_${i}_time.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f monpoly-linear "$INPUT_FILE" 2> "$DELAY_REPORT") \
                        | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$MONPOLY_EXE" -sig "$WORK_DIR/synthetic/synth.sig" -formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE -nonewlastts > "$VERDICT_FILE"

                fi

            done
        done
    done
done

echo "Dejavu standalone:"
for formula in $FORMULAS; do
    echo "  Evaluating $formula:"
    for er in $EVENT_RATES; do
        echo "    Event rate $er, index rate $er:"
        for acc in $ACCELERATIONS; do
            echo "    Acceleration $acc:"
            for i in $(seq 1 $REPETITIONS); do
                echo "      Repetition $i ..."

                fma=$("$DEJAVU_EXE" "compile" "$WORK_DIR/synthetic/$formula.mfotl.neg.qtl")

                if [[ "$acc" = "0" ]]; then

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.dvu"
                    TIME_REPORT="$REPORT_DIR/genCMP_dejavu_${formula}_${er}_${er}_${acc}_${i}_time.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    cat "$INPUT_FILE" | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$DEJAVU_EXE" "run" "$fma" 25 > "$VERDICT_FILE"

                else

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                    DELAY_REPORT="$REPORT_DIR/genCMP_dejavu_${formula}_${er}_${er}_${acc}_${i}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/genCMP_dejavu_${formula}_${er}_${er}_${acc}_${i}_time.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f dejavu-linear "$INPUT_FILE" 2> "$DELAY_REPORT") \
                        | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$DEJAVU_EXE" "run" "$fma" 25 > "$VERDICT_FILE"


                fi


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
            echo "      Event rate $er, index rate $er:"
            for acc in $ACCELERATIONS; do
                echo "    Acceleration $acc:"
                for i in $(seq 1 $REPETITIONS); do
                    echo "        Repetition $i ..."



                    if [[ "$acc" = "0" ]]; then

                        echo "          Monpoly ..."

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                        JOB_NAME="genCMP_flink_monpoly_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                        wait

                        echo "          Dejavu..."

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                        JOB_NAME="genCMP_flink_dejavu_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE_DEJAVU --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                        wait

                    else

                        echo "          Monpoly ..."

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                        JOB_NAME="genCMP_flink_monpoly_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                        wait

                        echo "          Dejavu..."

                        INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                        JOB_NAME="genCMP_flink_dejavu_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE_DEJAVU --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                        wait

                    fi


                done
            done

        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done



echo "Flink without checkpointing and with statistics:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        echo "    Evaluating $formula:"
        for er in $EVENT_RATES; do
            echo "      Event rate $er, index rate $er:"
            for acc in $ACCELERATIONS; do
                echo "    Acceleration $acc:"
                for i in $(seq 1 $REPETITIONS); do
                    echo "        Repetition $i ..."


                 if [[ "$acc" = "0" ]]; then

                    echo "          Monpoly ..."

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                    JOB_NAME="genCMP_flink_monpoly_stats_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait

                    echo "          Dejavu ..."

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                    JOB_NAME="genCMP_flink_dejavu_stats_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE_DEJAVU --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait


                 else

                    echo "          Monpoly ..."

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                    JOB_NAME="genCMP_flink_monpoly_stats_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE -nonewlastts $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait

                    echo "          Dejavu ..."

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${er}.csv"
                    JOB_NAME="genCMP_flink_dejavu_stats_${numcpus}_${formula}_${er}_${er}_${acc}_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm -r "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a $acc -q $REPLAYER_QUEUE -i csv -f csv-linear -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor dejavu --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $DEJAVU_EXE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" $NEGATE_DEJAVU --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait

                 fi


                done
            done

        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
echo "Scraping metrics from $start_time to $end_time ..."
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time genCMP)

echo
echo "Evaluation complete!"
