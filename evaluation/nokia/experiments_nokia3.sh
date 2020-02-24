#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

INPUT_TYPE="kafka sockets"
FORMULAS="ins-1-2-neg"
NEGATE=""
MULTISOURCE_VARIANTS="2"
KAFKA_PARTS="1"
PROCESSORS="16"
ACCELERATIONS="500 1000 2000"
MONPOLY_CPU_LIST="0"
MONPOLY_CMD=$MONPOLY_EXE
REORDER="yes"
cat "$ROOT_DIR/ldcc_sample.csv" | wc -l > "$REPORT_DIR/nokia.events"

VERDICT_FILE="$OUTPUT_DIR/verdicts.txt"

echo "=== Nokia experiments ==="

echo "Creating a monpoly version of the log ..."

if [[ -a "$ROOT_DIR/ldcc_sample.log" ]]; then
    echo "monpoly version already exists, skipping"
else
    "$WORK_DIR/replayer.sh" -a 0 -q "$REPLAYER_QUEUE" -i csv -f monpoly "$ROOT_DIR/ldcc_sample.csv" > "$ROOT_DIR/ldcc_sample.log"
fi

if [[ "$MONPOLY_CMD" == $MONPOLY_EXE ]]; then
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
fi

for variant in $MULTISOURCE_VARIANTS; do
    for numsources in $KAFKA_PARTS; do
        echo "Preprocessing for variant $variant and $numsources sources"
        PREPROCESSED_FILE="$STATE_DIR/preprocessed_${variant}_${numsources}"
        if [[ -a "${PREPROCESSED_FILE}_0.csv" ]]; then
            echo "alread preprocessed, skipping"
        else
            "$WORK_DIR"/trace-transformer.sh -v $variant -n $numsources -o "${PREPROCESSED_FILE}_" "$ROOT_DIR/ldcc_sample.csv"
        fi
    done
done

echo "finished preprocessing"

start_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

"$ZOOKEEPER_EXE" start &> /dev/null || fail "failed to start zookeeper"
sleep 3.0
"$KAFKA_BIN/kafka-server-start.sh" -daemon "$KAFKA_CONFIG_FILE" &> /dev/null &
"$FLINK_BIN/start-cluster.sh" &> /dev/null || fail "failed to start flink"
echo "Flink without checkpointing:"
for inp_type in $INPUT_TYPE; do
    for reorder in $REORDER; do
        echo "Reorder $reorder:"
        for numsources in $KAFKA_PARTS; do
            echo "  $numsources kafka part:"
            for procs in $PROCESSORS; do
                echo "      $procs processors:"
                for variant in $MULTISOURCE_VARIANTS; do
                    echo "          Variant $variant:"
                    for formula in $FORMULAS; do
                        echo "              Evaluating $formula:"
                        STATE_FILE="$STATE_DIR/ldcc_sample_past_${formula}.state"
                        for acc in $ACCELERATIONS; do
                            echo "                  Acceleration $acc:"
                                for i in $(seq 1 $REPETITIONS); do
                                    echo "                      Repetition $i ..."
                                    JOB_NAME="nokia_flink_monpoly_${inp_type}_${reorder}_${numsources}_${procs}_${variant}_${formula}_${acc}_${i}"
                                    DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                                    TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time_{ID}.txt"
                                    BATCH_TIME_REPORT="$REPORT_DIR/${JOB_NAME}_time.txt"
                                    JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"
                                    PREPROCESSED_FILE="$STATE_DIR/preprocessed_${variant}_${numsources}_"
                                    rm -r "$VERDICT_FILE" 2> /dev/null
                                    "$WORK_DIR/replayer.sh" --other_branch $(inp_type_replayer_args "$inp_type") -v $(variant_replayer_params $variant) -n $numsources -a $acc -q $REPLAYER_QUEUE -i csv -f csv -t 1000 "$PREPROCESSED_FILE" 2> "$DELAY_REPORT" &
                                    "$TIME_COMMAND" -f "%e;%M" -o "$BATCH_TIME_REPORT" "$WORK_DIR/monitor.sh" $(inp_type_flink_args "$inp_type") --format csv --out "$VERDICT_FILE" --monitor monpoly --command "$TIME_COMMAND -f %e;%M -o $TIME_REPORT $MONPOLY_CMD -nonewlastts $NEGATE" --sig "$WORK_DIR/nokia/ldcc.sig" --formula "$WORK_DIR/nokia/$formula.mfotl" --processors $procs --queueSize "$FLINK_QUEUE" --job "$JOB_NAME" --multi $variant --clear false --nparts $numsources $(reorder_to_flink_args "$reorder") $(monpoly_cmd_to_flink_args "$MONPOLY_CMD" "$STATE_FILE")  > "$JOB_REPORT"
                                    wait
                                done # reps
                        done # acc
                    done # formula
                done # variant
            done #procs
        done #numsources
    done #reorder
done #input type
#"$FLINK_BIN/stop-cluster.sh" &> /dev/null || fail "failed to stop flink"
#"$KAFKA_BIN/kafka-server-stop.sh" &> /dev/null || fail "failed to stop kafka"
#sleep 1.0
#"$ZOOKEEPER_EXE" stop &> /dev/null || fail "failed to stop zookeeper"

end_time=$(date +%Y-%m-%dT%H:%M:%S.%3NZ --utc)

echo
echo "Scraping metrics from $start_time to $end_time ..."
(cd "$REPORT_DIR" && "$WORK_DIR/scrape.sh" $start_time $end_time nokia)

echo
echo "Evaluation complete!"
