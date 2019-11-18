#!/usr/bin/env bash
BASE_DIR=$(dirname "${BASH_SOURCE[0]}")
java -jar "$BASE_DIR/trace-transformer/target/trace-transformer-1.0-SNAPSHOT.jar" "$@"
