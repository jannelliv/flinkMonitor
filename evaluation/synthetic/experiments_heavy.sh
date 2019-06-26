#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

FORMULAS="star-neg linear-neg triangle-neg"  # These must match the formulas in mk_heavy.py!
NEGATE="" # if formulas above are suffixed with -neg this should be "", otherwise "-negate"
EVENT_RATES="250 500 1000"
INDEX_RATES="1"
HEAVY_SETS_NO_STATS="h0 h1"
HEAVY_SETS_STATS="h1"
PROCESSORS="4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
AUX_CPU_LIST="10-11,34-35"
LOG_LENGTH=30

echo $LOG_LENGTH > "$REPORT_DIR/genh.length"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Synthetic experiments (heavy hitters) ==="

make_log() {
    flag=$1
    formula=$2
    heavy_set=$3
    exponents="$4"

    for er in $EVENT_RATES; do
        for ir in $INDEX_RATES; do
            "$WORK_DIR/generator.sh" $flag -e $er -i $ir -w 10 -pA 0.3333 -pB 0.3333 -z "$exponents" $LOG_LENGTH > "$OUTPUT_DIR/genh_${formula}_${heavy_set}_${er}_${ir}.csv"
        done
    done
}

echo "Generating logs ..."

make_log -S star-neg h0 "w=0,x=0,y=0,z=0"
make_log -S star-neg h1 "w=2,x=0,y=0,z=0"
make_log -S star-neg h2 "w=2,x=2,y=0,z=0"

make_log -L linear-neg h0 "w=0,x=0,y=0,z=0"
make_log -L linear-neg h1 "w=0,x=2,y=0,z=0"
make_log -L linear-neg h2 "w=0,x=2,y=2,z=0"

make_log -T triangle-neg h0 "x=0,y=0,z=0"
make_log -T triangle-neg h1 "x=2,y=0,z=0"
make_log -T triangle-neg h2 "x=2,y=2,z=0"

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

                        JOB_NAME="genh_flink_monpoly_ft_${numcpus}_${formula}_${heavy_set}_${er}_${ir}_1_${i}"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor monpoly -command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
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

                        JOB_NAME="genh_flink_monpoly_ft_stats_${numcpus}_${formula}_${heavy_set}_${er}_${ir}_1_${i}"
                        TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                        BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        rm -r "$VERDICT_FILE" 2> /dev/null
                        "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" --in "$INPUT_FILE" --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_EXE $NEGATE" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.3333,B=0.3333,C=0.3333" --heavy "$HEAVY_FILE" --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" > "$JOB_REPORT"
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
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time genh)

echo
echo "Evaluation complete!"
