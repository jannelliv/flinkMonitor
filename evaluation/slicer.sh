#!/usr/bin/env bash


# output file names (slices are suffixed with index 0 to num_slices-1, 
#                    strategy with _startegy, and statistics with _stats)

output=$1
num_slices=$2
shift
shift

WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
source "$WORK_DIR/config.sh"

tmpdir="$$"
mkdir -p "${ROOT_DIR}/${tmpdir}"
pushd "${ROOT_DIR}/${tmpdir}"
java -jar "$MONITOR_JAR" -calcSlice true -degree $num_slices "$@"
popd

for i in `seq 0 $((num_slices-1))`; do
    mv "${ROOT_DIR}/${tmpdir}/slicedOutput${i}" "${output}${i}"
done
mv "${ROOT_DIR}/${tmpdir}/strategyOutput" "${output}_strategy"
mv "${ROOT_DIR}/${tmpdir}/otherStuff" "${output}_stats"

rm -rf "${ROOT_DIR}/${tmpdir}"

