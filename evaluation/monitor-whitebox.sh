#!/usr/bin/env bash


WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
source "$WORK_DIR/config.sh"
exec "$FLINK_BIN/flink" run "$MONITOR_WHITEBOX_JAR" "$@"
