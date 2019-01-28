#!/usr/bin/env bash


# output file names (slices are suffixed with index 0 to num_slices-1, 
#                    strategy with _startegy, and statistics with _stats)

output=$1
num_slices=$2
shift
shift

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
source "$WORK_DIR/config.sh"
java -jar "$MONITOR_JAR" -calcSlice true -degree $num_slices "$@"

for i in `seq 0 $((num_slices-1))`; do
    mv "${ROOT_DIR}/slicedOutput${i}" "${output}${i}"
done
mv "${ROOT_DIR}/strategyOutput" "${output}_strategy"
mv "${ROOT_DIR}/otherStuff" "${output}_stats"

