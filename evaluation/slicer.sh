#!/usr/bin/env bash


# output file names (slices are suffixed with index 0 to num_slices-1, 
#                    strategy with _startegy, and statistics with _stats)

output=$1
shift
num_slices=$2
shift

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
source "$WORK_DIR/config.sh"
exec java -cp "$MONITOR_JAR" -calcSlice true -format csv  -degree $num_slices "$@"

for i in `seq 0 $((num_slices-1))`; do
    mv slicedOutput${i} ${output}${i}
done
mv strategyOutput ${output}_strategy
mv otherStuff ${output}_stats

