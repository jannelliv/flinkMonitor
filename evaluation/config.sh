#!/usr/bin/env bash

[[ -n $WORK_DIR ]] || WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
PROJECT_DIR=`cd "$WORK_DIR/.."; pwd`
ROOT_DIR=`cd "$WORK_DIR/../.."; pwd`

export JAVA_HOME="$ROOT_DIR/jdk-10.0.2"
export PATH="$PATH:$WORK_DIR/visual/sqlite"

FLINK_BIN="$ROOT_DIR/flink/bin"
MONITOR_JAR="$PROJECT_DIR/target/parallel-online-monitoring-1.0-SNAPSHOT.jar"
REPLAYER_JAR="$PROJECT_DIR/replayer/target/replayer-1.0-SNAPSHOT.jar"
TOOL_JAR="$PROJECT_DIR/tools/target/evaluation-tools-1.0-SNAPSHOT.jar"

CHECKPOINT_DIR="$ROOT_DIR/checkpoints"
OUTPUT_DIR="$ROOT_DIR/output"
REPORT_DIR="$ROOT_DIR/reports"
EXEC_LOG_DIR="$ROOT_DIR/logs"

TIME_COMMAND=/usr/bin/time

STREAM_PORT=10101
