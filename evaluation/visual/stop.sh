#!/usr/bin/env bash

SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`

if [[ -f ${SCRIPT_DIR}/setup.sh ]]; then
    echo "You must call stop.sh from the intallation folder"
    exit 1
fi

echo ${SCRIPT_DIR}/grafana.pid | xargs kill
echo ${SCRIPT_DIR}/prometheus.pid | xargs kill

