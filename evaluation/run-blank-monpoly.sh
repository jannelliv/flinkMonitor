#!/bin/bash
# Script runs a montor that never outputs any violations,
# but only propagates Monpoly's synchronization barriers.
# This script is used for testing and debugging purposes.

# The script assumes that GNU grep and awk are available on the PATH

# Usage:
# <flink run command> --comand "./run-blank-monpoly.sh"

grep --line-buffered get_pos | awk '// { print "Current timepoint: 1"; fflush(stdout)}'
