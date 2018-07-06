#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="star linear triangle"
EVENT_RATES="2000 2500 3000 3500 4000 5000 6000 8000"
INDEX_RATES="1000"
HEAVY_SETS_NO_STATS="h0 h1"
HEAVY_SETS_STATS="h1"
PROCESSORS="4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
AUX_CPU_LIST="10-11,34-35"
REPLAYER_QUEUE=1200

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Synthetic experiments (heavy hitters) ==="

make_log() {
    flag=$1
    formula=$2
    heavy_set=$3
    exponents="$4"

    for er in $EVENT_RATES; do
        for ir in $INDEX_RATES; do
            "$WORK_DIR/generator.sh" $flag -e $er -i $ir -x 10 -w 10 -pA 0.3333 -pB 0.3333 -z "$exponents" 60 > "$OUTPUT_DIR/genh_${formula}_${heavy_set}_${er}_${ir}.csv"
        done
    done
}

echo "Generating logs ..."

make_log -S star h0 "0,0,0,0"
make_log -S star h1 "2,0,0,0"
make_log -S star h2 "2,2,0,0"

make_log -L linear h0 "0,0,0,0"
make_log -L linear h1 "0,2,0,0"
make_log -L linear h2 "0,2,2,0"

make_log -T triangle h0 "0,0,0"
make_log -T triangle h1 "2,0,0"
make_log -T triangle h2 "2,2,0"

(cd "$OUTPUT_DIR"; "$WORK_DIR/synthetic/mk_heavy.py")


start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo "Without statistics:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        for heavy_set in $HEAVY_SETS_NO_STATS; do
            echo "    Evaluating $formula ($heavy_set):"
            for er in $EVENT_RATES; do
                for ir in $INDEX_RATES; do
                echo "      Event rate $er, index rate $ir:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."

                        INPUT_FILE="$OUTPUT_DIR/genh_${formula}_${heavy_set}_${er}_${ir}.csv"

                        JOB_NAME="genh_flink_ft_${numcpus}_${formula}_${heavy_set}_${er}_${ir}_${i}"
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
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "With statistics:"
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    echo "  $numcpus processors:"

    taskset -c $cpulist "$FLINK_BIN/start-cluster.sh" > /dev/null

    for formula in $FORMULAS; do
        for heavy_set in $HEAVY_SETS_STATS; do
            echo "    Evaluating $formula ($heavy_set):"
            for er in $EVENT_RATES; do
                for ir in $INDEX_RATES; do
                echo "      Event rate $er, index rate $ir:"
                    for i in $(seq 1 $REPETITIONS); do
                        echo "        Repetition $i ..."

                        INPUT_FILE="$OUTPUT_DIR/genh_${formula}_${heavy_set}_${er}_${ir}.csv"
                        HEAVY_FILE="$OUTPUT_DIR/heavy_${numcpus}_${formula}_${heavy_set}.csv"

                        JOB_NAME="genh_flink_ft_stats_${numcpus}_${formula}_${heavy_set}_${er}_${ir}_${i}"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm "$VERDICT_FILE" 2> /dev/null
                        taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                        "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.3333,B=0.3333,C=0.3333" --heavy "$HEAVY_FILE" --job "$JOB_NAME" > "$JOB_REPORT"
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
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time gen_heavy)

echo
echo "Evaluation complete!"
