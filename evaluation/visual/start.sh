#!/usr/bin/env bash

SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
DATASOURCE="monitor"

if [[ -f ${SCRIPT_DIR}/setup.sh ]]; then
    echo "You must call start.sh from the intallation folder"
    exit 1
fi

source ${SCRIPT_DIR}/../config.sh

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
    pushd ${SCRIPT_DIR}/grafana 
    ./bin/grafana-server web 2> /dev/null > /dev/null  &
    PIDG=$!
    echo $PIDG > ${SCRIPT_DIR}/grafana.pid
    popd
    echo "Starting Grafana..."
    sleep 2
    response=$(curl --write-out %{http_code} --silent --output /dev/null http://admin:admin@localhost:6001/api/datasources/name/$DATASOURCE)
    if [[ ! $response -eq 200 ]]; then
        sed -i "s/SOURCENAME/$DATASOURCE/g" ${SCRIPT_DIR}/create.json
        response=$(curl -X POST -H "Content-Type: application/json" -d @${SCRIPT_DIR}/create.json --write-out %{http_code} --silent --output /dev/null http://admin:admin@localhost:6001/api/datasources)
        if [[ ! $response -eq 200 ]]; then
            echo "Prometheus source added successfully."
        else
            echo "Prometheus source could not be added, please try to add it manually."
        fi
    fi
else
    echo "Grafana not installed."
    exit 1
fi
