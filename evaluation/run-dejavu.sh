#!/bin/bash
# Script runs Dejavu and writes its input and output
# to files in the current directory. Input files are 
# named dejavu_slice<ID>.csv where the <ID> is replaced
# with the ID of the Dejavu parallel instance.
# Output files follow a similar convention.
# This script is used for testing and debugging purposes.

# The script assumes that dejavu script is available on the PATH

# Usage:
# <flink run command> --comand "./run-dejavu.sh {ID} [dejavu commands]"


id=$1
shift
path=`pwd`
tee $path/dejavu_slice$id.csv | dejavu "$@" | tee $path/dejavu_out$id.csv
