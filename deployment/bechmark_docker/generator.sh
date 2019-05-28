#!/usr/bin/env bash
BASE_DIR=$(dirname "${BASH_SOURCE[0]}")
java -jar "$BASE_DIR/trace-generator-1.0-SNAPSHOT.jar" "$@"
