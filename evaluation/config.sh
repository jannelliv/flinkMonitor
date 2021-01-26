#!/usr/bin/env bash

[[ -n $WORK_DIR ]] || WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
PROJECT_DIR=`cd "$WORK_DIR/.."; pwd`
ROOT_DIR=`cd "$WORK_DIR/../.."; pwd`

export JAVA_HOME="$ROOT_DIR/jdk1.8.0_211"
export PATH="$PATH:$ROOT_DIR/scala-2.12.7/bin/"
export PATH="$PATH:$WORK_DIR/visual/sqlite"

FLINK_BIN="$ROOT_DIR/flink/bin"
KAFKA_BIN="$ROOT_DIR/kafka/bin"
ZOOKEEPER_BIN="$ROOT_DIR/zookeeper/bin"
ZOOKEEPER_EXE="$ROOT_DIR/zookeeper/bin/zkServer.sh"
KAFKA_CONFIG_FILE="$ROOT_DIR/kafka/config/server.properties"

TRACE_TRANSFORMER_JAR="$PROJECT_DIR/trace-transformer/target/trace-transformer-1.0-SNAPSHOT.jar"
MONITOR_JAR="$PROJECT_DIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
MONITOR_WHITEBOX_JAR="$PROJECT_DIR/flink-monitor/target/flink-implementation-1.0-SNAPSHOT.jar"

REPLAYER_JAR="$PROJECT_DIR/replayer/target/replayer-1.0-SNAPSHOT.jar"
TOOL_JAR="$PROJECT_DIR/trace-generator/target/trace-generator-1.0-SNAPSHOT.jar"
DEJAVU_EXE="$PROJECT_DIR/evaluation/dejavu/dejavu"
MONPOLY_EXE="$ROOT_DIR/monpoly/monpoly"
BLANK_MONPOLY_EXE="$WORK_DIR/run-blank-monpoly.sh"

CHECKPOINT_DIR="$ROOT_DIR/checkpoints"
OUTPUT_DIR="$ROOT_DIR/output"
REPORT_DIR="$ROOT_DIR/reports"
EXEC_LOG_DIR="$ROOT_DIR/logs"
STATE_DIR="$ROOT_DIR/state"

TIME_COMMAND=/usr/bin/time

REPETITIONS=1
FLINK_QUEUE=256
REPLAYER_QUEUE=2000

STREAM_PORT=10101

fail() {
    echo "ERROR: $1"
    exit 1
}

monpoly_cmd_to_string() {
    if [[ "$1" == "$MONPOLY_EXE" ]]; then
        echo "normalcmd"
    elif [[ "$1" == "$BLANK_MONPOLY_EXE" ]]; then
        echo "blankcmd"
    else
        fail "unknown monpoly exe"
    fi
}

monpoly_cmd_to_flink_args(){
    if [[ "$1" == "$MONPOLY_EXE" ]]; then
        echo "--load $2"
    elif [[ "$1" == "$BLANK_MONPOLY_EXE" ]]; then
        echo ""
    else
        fail "unknown monpoly exe"
    fi
}

reorder_to_flink_args(){
    if [[ "$1" == "yes" ]]; then
        echo ""
    elif [[ "$1" == "no" ]]; then
        echo "--skipreorder"
    else
        fail "unknown reorder mode"
    fi
}

inp_type_out_flag(){
    if [[ "$1" == "sockets" ]]; then
        echo "127.0.0.1:6060"
    elif [[ "$1" == "kafka" ]]; then
        echo "kafka"
    else
        fail "unknown reorder mode"
    fi
}

inp_type_in_flag(){
    if [[ "$1" == "sockets" ]]; then
        echo "127.0.0.1:6060"
    elif [[ "$1" == "kafka" ]]; then
        echo "kafka"
    else
        fail "unknown reorder mode"
    fi
}

variant_replayer_params() {
    if [[ "$1" == "1" ]]; then
        echo "--term TIMEPOINTS"
    elif [[ "$1" == "2" ]]; then
        echo "--term TIMESTAMPS"
    elif [[ "$1" == "4" ]]; then
        echo "--term NO_TERM -e"
    else
        fail "unknown multisource variant"
    fi
}

clear_topic() {
    "$KAFKA_BIN/kafka-topics.sh" --zookeeper localhost:2181 --delete --topic monitor_topic &> /dev/null
    "$KAFKA_BIN/kafka-topics.sh" --zookeeper localhost:2181 --create --topic monitor_topic --partitions "$1" --replication-factor 1 &> /dev/null
}




