#!/usr/bin/env bash

# This script scrapes data from prometheus in the fixed 
# data ranges when we ran the experiments

# Example usage 
# ./scrape-all.sh 

SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
TARGET_DIR=`pwd`

# Nokia data range:
$SCRIPT_DIR/scrape-all.sh 2018-06-22T22:00:00.000Z  2018-06-23T10:00:00.000Z nokia

# Synthethic data range:
$SCRIPT_DIR/scrape-all.sh 2018-06-24T09:00:00.000Z  2018-06-25T10:00:00.000Z synthethic
$SCRIPT_DIR/scrape-all.sh 2018-07-04T20:30:00.000Z  2018-07-05T05:30:00.000Z synthethic-part2

# Heavy hitters data range:
$SCRIPT_DIR/scrape-all.sh 2018-06-28T16:30:00.000Z  2018-06-29T00:15:00.000Z hh-part1
$SCRIPT_DIR/scrape-all.sh 2018-06-29T17:50:00.000Z  2018-06-29T19:00:00.000Z hh-part2
$SCRIPT_DIR/scrape-all.sh 2018-06-30T13:00:00.000Z  2018-06-30T16:00:00.000Z hh-part3
