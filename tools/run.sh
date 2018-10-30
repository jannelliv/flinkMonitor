#!/usr/bin/env bash
BASE_DIR=$(dirname "${BASH_SOURCE[0]}")
cmd=$1
shift

case "$cmd" in
  generator|Generator ) $BASE_DIR/generator.sh $@;;
  replayer|Replayer ) $BASE_DIR/replayer.sh $@;;
  * ) echo "invalid command";;
esac
