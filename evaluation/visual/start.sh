#!/usr/bin/env bash

SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`

if [[ -f ${SCRIPT_DIR}/setup.sh ]]; then
    echo "You must call start.sh from the intallation folder"
    exit 1
fi

source ../config.sh

if [[ -d ${SCRIPT_DIR}/prometheus ]]; then
    ${SCRIPT_DIR}/prometheus/prometheus --config.file=${SCRIPT_DIR}/prometheus/prometheus.yml 2> /dev/null &
    PIDP=$!
    echo $PIDP > ${SCRIPT_DIR}/prometheus.pid
    echo "Starting Prometheus..."
else
    echo "Prometheus not installed."
    exit 1
fi

if [[ -d ${SCRIPT_DIR}/grafana ]]; then
    (cd ${SCRIPT_DIR}/grafana ; ./bin/grafana-server web) &
    PIDG=$!
    echo $PIDG > ${SCRIPT_DIR}/grafana.pid
    echo "Starting Grafana..."
else
    echo "Grafana not installed."
    exit 1
fi
