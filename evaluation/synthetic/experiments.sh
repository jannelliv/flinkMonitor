#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="star linear triangle"
EVENT_RATES="2000 2500 3000 4000 6000 8000 10000 12000"
INDEX_RATES="1 1000"
PROCESSORS="1/0-2,24-26 4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
REPLAYER_QUEUE=1200

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Synthetic experiments (relation sizes) ==="

make_log() {
    flag=$1
    formula=$2
    for er in $EVENT_RATES; do
        for ir in $INDEX_RATES; do
            "$WORK_DIR/generator.sh" $flag -e $er -i $ir -x 10 -w 10 -pA 0.01 -pB 0.495 60 > "$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
        done
    done
}

echo "Generating logs ..."
make_log -S star
make_log -L linear
make_log -T triangle


#echo "Monpoly standalone:"
#for formula in $FORMULAS; do
#    echo "  Evaluating $formula:"
#    for er in $EVENT_RATES; do
#        for ir in $INDEX_RATES; do
#        echo "    Event rate $er, index rate $ir:"
#            for i in $(seq 1 $REPETITIONS); do
#                echo "      Repetition $i ..."
#
#                INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
#                DELAY_REPORT="$REPORT_DIR/gen_monpoly_${formula}_${er}_${ir}_${i}_delay.txt"
#                TIME_REPORT="$REPORT_DIR/gen_monpoly_${formula}_${er}_${ir}_${i}_time.txt"
#
#                rm "$VERDICT_FILE" 2> /dev/null
#                (taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -m "$INPUT_FILE" 2> "$DELAY_REPORT") \
#                    | taskset -c $MONPOLY_CPU_LIST "$TIME_COMMAND" -f "%e;%M" -o "$TIME_REPORT" "$WORK_DIR/monpoly" -sig "$WORK_DIR/synthetic/synth.sig" -formula "$WORK_DIR/synthetic/$formula.mfotl" -negate > "$VERDICT_FILE"
#            done
#        done
#    done
#done

start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

#echo "Flink without checkpointing:"
#for procs in $PROCESSORS; do
#    numcpus=${procs%/*}
#    cpulist=${procs#*/}
#    echo "  $numcpus processors:"
#
#    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null
#
#    for formula in $FORMULAS; do
#        echo "    Evaluating $formula:"
#        for er in $EVENT_RATES; do
#            for ir in $INDEX_RATES; do
#            echo "      Event rate $er, index rate $ir:"
#                for i in $(seq 1 $REPETITIONS); do
#                    echo "        Repetition $i ..."
#
#                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
#                    JOB_NAME="gen_flink_${numcpus}_${formula}_${er}_${ir}_${i}"
#                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
#                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
#                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
#
#                    rm "$VERDICT_FILE" 2> /dev/null
#                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
#                    "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
#                    wait
#                done
#            done
#        done
#    done
#
#    "$FLINK_BIN/stop-cluster.sh" > /dev/null
#done

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
                for i in $(seq 1 $REPETITIONS); do
                    echo "        Repetition $i ..."

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                    JOB_NAME="gen_flink_ft_${numcpus}_${formula}_${er}_${ir}_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                    wait
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
                for i in $(seq 1 $REPETITIONS); do
                    echo "        Repetition $i ..."

                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                    JOB_NAME="gen_flink_ft_stats_${numcpus}_${formula}_${er}_${ir}_${i}"
                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                    rm "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --job "$JOB_NAME" > "$JOB_REPORT"
                    wait
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
