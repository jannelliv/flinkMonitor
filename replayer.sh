#!/usr/bin/env bash
BASE_DIR=$(dirname "${BASH_SOURCE[0]}")
java -jar "$BASE_DIR/replayer/target/replayer-1.0-SNAPSHOT.jar" "$@"
