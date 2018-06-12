#!/usr/bin/env bash

WORK_DIR=`cd "${BASH_SOURCE%/*}"; pwd`
source "$WORK_DIR/config.sh"
exec "$FLINK_BIN/flink" run "$MONITOR_JAR" "$@"
