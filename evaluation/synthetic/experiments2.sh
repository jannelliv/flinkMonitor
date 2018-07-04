#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

REPETITIONS=3
FORMULAS="star linear triangle"
EVENT_RATES="8000 12000 30000"
INDEX_RATES="1 1000"
PROCESSORS="4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
MONPOLY_CPU_LIST="0"
AUX_CPU_LIST="10-11,34-35"
REPLAYER_QUEUE=1200
TIMEOUT=150

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

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


cancel_job() {
    echo "Timeout, canceling job $1"
    "$FLINK_BIN/flink" cancel $("$FLINK_BIN/flink" list | perl -n -e '/: ([a-z0-9]+) : .* \(RUNNING\)/ && print $1') > /dev/null 2> /dev/null
}

start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo "Flink with Monpoly, no checkpointing:"
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

                    JOB_NAME="gen_flink_${numcpus}_${formula}_${er}_${ir}_${i}"
                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                    DELAY_REPORT="$REPORT_DIR/gen_flink_${numcpus}_${formula}_${er}_${ir}_${i}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/gen_flink_${numcpus}_${formula}_${er}_${ir}_${i}_time_{ID}.txt"
                    JOB_REPORT="$REPORT_DIR/gen_flink_${numcpus}_${formula}_${er}_${ir}_${i}_job.txt"

                    rm "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    timeout $TIMEOUT "$WORK_DIR/monitor.sh" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                    if [[ $? = 124 ]]; then
                        cancel_job "$JOB_NAME"
                    fi
                done
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "Flink with Monpoly, checkpointing:"
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

                    JOB_NAME="gen_flink_ft_${numcpus}_${formula}_${er}_${ir}_${i}"
                    INPUT_FILE="$OUTPUT_DIR/gen_${formula}_${er}_${ir}.csv"
                    DELAY_REPORT="$REPORT_DIR/gen_flink_ft_${numcpus}_${formula}_${er}_${ir}_${i}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/gen_flink_ft_${numcpus}_${formula}_${er}_${ir}_${i}_time_{ID}.txt"
                    JOB_REPORT="$REPORT_DIR/gen_flink_ft_${numcpus}_${formula}_${er}_${ir}_${i}_job.txt"

                    rm "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    timeout $TIMEOUT "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --job "$JOB_NAME" > "$JOB_REPORT"
                    if [[ $? = 124 ]]; then
                        cancel_job "$JOB_NAME"
                    fi
                done
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

echo "Flink with Monpoly, checkpointing, and statistics:"
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
                    DELAY_REPORT="$REPORT_DIR/gen_flink_ft_stats_${numcpus}_${formula}_${er}_${ir}_${i}_delay.txt"
                    TIME_REPORT="$REPORT_DIR/gen_flink_ft_stats_${numcpus}_${formula}_${er}_${ir}_${i}_time_{ID}.txt"
                    JOB_REPORT="$REPORT_DIR/gen_flink_ft_stats_${numcpus}_${formula}_${er}_${ir}_${i}_job.txt"

                    rm "$VERDICT_FILE" 2> /dev/null
                    taskset -c $AUX_CPU_LIST "$WORK_DIR/replayer.sh" -v -a 1 -q $REPLAYER_QUEUE -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                    timeout $TIMEOUT "$WORK_DIR/monitor.sh" --checkpoints "file://$CHECKPOINT_DIR" --in localhost:$STREAM_PORT --format csv --out "$VERDICT_FILE" --monitor "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $WORK_DIR/monpoly -negate" --sig "$WORK_DIR/synthetic/synth.sig" --formula "$WORK_DIR/synthetic/$formula.mfotl" --processors $numcpus --rates "A=0.01,B=0.495,C=0.495" --job "$JOB_NAME" > "$JOB_REPORT"
                    if [[ $? = 124 ]]; then
                        cancel_job "$JOB_NAME"
                    fi
                done
            done
        done
    done

    "$FLINK_BIN/stop-cluster.sh" > /dev/null
done

end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
#echo "Scraping metrics..."
#(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time synthetic)
echo "Metrics period: $start_time $end_time"

echo
echo "Evaluation complete!"
