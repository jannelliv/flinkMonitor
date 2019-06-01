#!/usr/bin/env bash

[[ -n $WORK_DIR ]] || WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
PROJECT_DIR=`cd "$WORK_DIR/.."; pwd`
ROOT_DIR=`cd "$WORK_DIR/../.."; pwd`

export JAVA_HOME="$ROOT_DIR/jdk1.8.0"
export PATH="$PATH:$WORK_DIR/visual/sqlite"

FLINK_BIN="$ROOT_DIR/flink/bin"
MONITOR_JAR="$PROJECT_DIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
REPLAYER_JAR="$PROJECT_DIR/replayer/target/replayer-1.0-SNAPSHOT.jar"
TOOL_JAR="$PROJECT_DIR/trace-generator/target/trace-generator-1.0-SNAPSHOT.jar"
DEJAVU_EXE="$PROJECT_DIR/evaluation/dejavu/dejavu"
MONPOLY_EXE="$ROOT_DIR/monpoly/monpoly"

CHECKPOINT_DIR="$ROOT_DIR/checkpoints"
OUTPUT_DIR="$ROOT_DIR/output"
REPORT_DIR="$ROOT_DIR/reports"
EXEC_LOG_DIR="$ROOT_DIR/logs"

TIME_COMMAND=/usr/bin/time

STREAM_PORT=10101




