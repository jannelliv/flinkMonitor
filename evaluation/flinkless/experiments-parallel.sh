#!/usr/bin/env bash

# Experiments that run MonPoly without Flink

# EXPERIMENT PARAMETERS:
REPETITIONS=3
FORMULAS="-S -L -T"
EVENT_RATES="2000"  #"2000 4000"
INDEX_RATES="1"     #"1 100"
LOG_LENGTH="1000"
PROCESSORS="4/0-5,24-29 8/0-9,24-33"  #"16/0-8,12-20,24-32,36-44"
NUM_ADAPTATIONS='2/1/-x 0.01;-x 0.01;-pA 0.01 -pB 0.495 -x 0.01#2/2/-x 0.01;-x 0.01;-pA 0.495 -pB 0.495 -x 0.005#2/3/-x 0.01;-x 0.01;-pA 0.01 -pB 0.01 -x 0.01#2/4/-x 0.01;-x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01#2/5/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-x 0.01#2/6/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+2000,y=0,z=0,w=0 -x 0.01#2/7/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=2+1000,y=0,z=0,w=0 -x 0.01#2/8/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=0,y=10+1000,z=0,w=0 -x 0.01#2/9/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=10+2000,z=0,w=0 -x 0.01'
WINDOW=10

# Log generation strategies 
# Equal relation ratios and uniform value distribution (i.e., -pA 0.3333 -pB 0.3333 -z x=0,y=0,z=0,w=0)
# Params                                                                                                        |  Explanation
# num_adapt/ID/strategies                                                                                       |
# -----------------------------------------------------------------------------------------------------------------------------------------------------
#2/1/-x 0.01;-x 0.01;-pA 0.01 -pB 0.495 -x 0.01                                                                 |  change relation rates (less A)
#2/2/-x 0.01;-x 0.01;-pA 0.495 -pB 0.495 -x 0.005                                                               |  change relation rates (less C)
#2/3/-x 0.01;-x 0.01;-pA 0.01 -pB 0.01 -x 0.01                                                                  |  change relation rates (less A and B)
#2/4/-x 0.01;-x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01                                                           |  introduce a single HH value
#2/5/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-x 0.01                                  |  remove a single HH value
#2/6/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+2000,y=0,z=0,w=0 -x 0.01         |  change a single HH value
#2/7/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=2+1000,y=0,z=0,w=0 -x 0.01          |  change the number of HH values
#2/8/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=0,y=10+1000,z=0,w=0 -x 0.01         |  change the HH variable
#2/9/-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=0,z=0,w=0 -x 0.01;-z x=10+1000,y=10+2000,z=0,w=0 -x 0.01   |  change the number of HH variables


# PREAMBLE & PRIVATE PARAMETERS
WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

IN_PIPE="/tmp/in"
OUT_PIPE="/tmp/out"
MONPOLY=$ROOT_DIR/monpoly/monpoly
SILENT=false
DEBUG=false
EXP_NAME="genadaptive"
SKIP_GENERATE=false
SKIP_MONITOR=false
PARALLELISM=4
declare -A TIMEMAP





function parse_options() {
    local option
    while [[ $# -gt 0 ]]; do
        option="$1"
        shift
        case ${option} in
            -h|-H|--help)
                usage
                exit 0
                ;;
            -v|-V|--verbose)
                DEBUG=true
                ;;
            -s|-S|--silent)
                SILENT=true
                ;;
            -m|-M|--monitor)
                SKIP_GENERATE=true
                ;;
            -g|-G|--generate)
                SKIP_MONITOR=true
                ;;
            -n|-N|--name)
                if [ ! -z "$1" ]; then 
                    EXP_NAME=$1 
                else 
                    echo "Invalid argument was provided: ${option}"
                    usage
                    exit 1
                fi
                shift
                ;;
            -p|-P|--parallelism)
                if [ ! -z "$1" ]; then 
                    PARALLELISM=$1 
                else 
                    echo "Invalid argument was provided: ${option}" 
                    usage 
                    exit 1
                fi
                shift
                ;;
            *)
                echo "Invalid argument was provided: ${option}"
                usage
                exit 1
                ;;
        esac
    done
}

function usage() {
    script_name="$(basename "${BASH_SOURCE[0]}")"
    script_version="1.0.0"
    
cat << EOF
Usage: ${script_name} [OPTION]...
Runs adaptive monitoring experiments with MonPoly in parallel and without Flink.
Edit the topmost uppercase variables in order to change parameters of the experiments.
Version: ${script_version}
Options:
  -h|--help                 Displays this help
  -s|--silent               Displays no output
  -v|--verbose              Displays debug output
  -n|--name NAME            Sets the name of the experiment (default: genadaptive)
  -m|--monitor              Skips generating the logs (runs the monitor assuming that logs exist)
  -g|--generate             Skips monitoring the logs (runs only log the generation)
  -p|--parallelism NUMBER   Non-negative umber of CPUs to be used for running the experiments (default: 4)
EOF
}

function debug() {
    if [[ ${DEBUG} == "true" ]]; then
        echo "[DEBUG]" "$@" 1>&2
    fi
}

function info() {
    if [[ ${SILENT} != "true" ]]; then
        echo "[INFO]" "$@" 1>&2
    fi
}

function error() {
    if [[ ${SILENT} != "true" ]]; then
        echo "[ERROR]" "$@" 1>&2
    fi
    exit 0
}




parse_options "$@"

if [[ ${SKIP_GENERATE} == "false" ]]; then

    info "=== Generating logs ==="
    #find $OUTPUT_DIR -type f -delete
    find $EXEC_LOG_DIR -type f -delete
    
    parallel --no-notice -P $PARALLELISM "$WORK_DIR/flinkless/run.sh" -g ::: `eval echo $FORMULAS` ::: `eval echo $EVENT_RATES` ::: `eval echo $INDEX_RATES` ::: "$NUM_ADAPTATIONS" ::: "$PROCESSORS" ::: "$LOG_LENGTH" ::: "$WINDOW" > "${EXEC_LOG_DIR}/execution.log" 2>&1 
    #echo parallel --no-notice -P $PARALLELISM "$WORK_DIR/flinkless/run.sh" -g ::: `eval echo $FORMULAS` ::: `eval echo $EVENT_RATES` ::: `eval echo $INDEX_RATES` ::: "$NUM_ADAPTATIONS" ::: "$PROCESSORS" ::: "$LOG_LENGTH" ::: "$WINDOW" #> "${EXEC_LOG_DIR}/execution.log"
    
fi


if [[ ${SKIP_MONITOR} == "false" ]]; then

info "=== Running flinkless experiments ==="

    rm -f ${IN_PIPE}*
    rm -f ${OUT_PIPE}*
    find $REPORT_DIR -type f -delete
    find $CHECKPOINT_DIR -type f -delete
    find $EXEC_LOG_DIR -type f -delete

    CPUs=""
    for procs in $PROCESSORS; do
        numcpus=${procs%/*}
        cpulist=${procs#*/}
        CPUs="${CPUs} ${numcpus}"
    done

    parallel --no-notice -P $PARALLELISM "$WORK_DIR/flinkless/run.sh" -m ::: `eval echo $FORMULAS` ::: `eval echo $EVENT_RATES` ::: `eval echo $INDEX_RATES` ::: `eval echo $CPUs` ::: "$NUM_ADAPTATIONS" ::: "$REPETITIONS" > "${EXEC_LOG_DIR}/execution.log" 2>&1 
    #echo parallel --no-notice -P $PARALLELISM "$WORK_DIR/flinkless/run.sh" -m ::: `eval echo $FORMULAS` ::: `eval echo $EVENT_RATES` ::: `eval echo $INDEX_RATES` ::: `eval echo $CPUs` ::: "$NUM_ADAPTATIONS" ::: "$REPETITIONS" #> "${EXEC_LOG_DIR}/execution.log"
fi


                
