#!/usr/bin/env bash

SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`

if [[ -f ${SCRIPT_DIR}/setup.sh ]]; then
    echo "You must call stop.sh from the intallation folder"
    exit 1
fi

ps -af | grep grafana | tr -s " " | cut -d " " -f2 | xargs kill
ps -af | grep prometheus | tr -s " " | cut -d " " -f2 | xargs kill