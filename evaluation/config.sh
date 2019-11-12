#!/usr/bin/env bash

[[ -n $WORK_DIR ]] || WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
PROJECT_DIR=`cd "$WORK_DIR/.."; pwd`
ROOT_DIR=`cd "$WORK_DIR/../.."; pwd`

export JAVA_HOME="$ROOT_DIR/jdk1.8.0_211"
export PATH="$PATH:$ROOT_DIR/scala-2.12.7/bin/"
export PATH="$PATH:$WORK_DIR/visual/sqlite"

KAFKA_BIN="$ROOT_DIR/kafka/bin"
FLINK_BIN="$ROOT_DIR/flink/bin"
ZOOKEEPER_BIN="$ROOT_DIR/zookeeper/bin"
MONITOR_JAR="$PROJECT_DIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
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

REPETITIONS=3
FLINK_QUEUE=256
REPLAYER_QUEUE=2000

STREAM_PORT=10101




