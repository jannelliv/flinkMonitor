#!/bin/bash
# Script runs a montor that never outputs any violations,
# but only propagates Dejavu's synchronization barriers.
# This script is used for testing and debugging purposes.

# The script assumes that GNU grep and awk are available on the PATH

# Usage:
# <flink run command> --comand "./run-blank-dejavu.sh"

grep --line-buffered SYNC | awk '// { print "**** SYNC!"; fflush(stdout)}'
