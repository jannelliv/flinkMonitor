#!/bin/bash
# Script runs Monpoly and writes its input and output
# to files in the current directory. Input files are 
# named monpoly_slice<ID>.log where the <ID> is replaced
# with the ID of the Monpoly parallel instance.
# Output files follow a similar convention.
# This script is used for testing and debugging purposes.

# The script assumes that monpoly is available on the PATH

# Usage:
# <flink run command> --comand "./run-monpoly.sh {ID} [monpoly commands]"

id=$1
shift
path=`pwd`

tee $path/monpoly_slice$id.log | monpoly "$@" | tee $path/monpoly_out$id.log
