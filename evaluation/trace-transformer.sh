#!/usr/bin/env bash

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
source "$WORK_DIR/config.sh"
exec java -jar "$TRACE_TRANSFORMER_JAR" "$@"

