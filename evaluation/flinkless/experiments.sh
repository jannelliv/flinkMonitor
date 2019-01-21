#!/usr/bin/env bash

# Experiments that run MonPoly without Flink

# EXPERIMENT PARAMETERS:
REPETITIONS=3
FORMULAS="-S -L -T"
EVENT_RATES="2000 2500 3000 3500 4000 5000 6000 8000"
INDEX_RATES="1000"
LOG_LENGTH="60"
HEAVY_SETS_NO_STATS="h0 h1"
HEAVY_SETS_STATS="h1"
PROCESSORS="4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
NUM_ADAPTATIONS="1/s1;s2 2/s1;s2;s3 3/s1;s2;s3;s4"
WINDOW=10
VIOLATIONS=0.1

# AUX_CPU_LIST="10-11,34-35"
# REPLAYER_QUEUE=1200

# PREAMBLE & PRIVATE PARAMETERS
WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

IN_PIPE="/tmp/in"
OUT_PIPE="/tmp/out"
REPORT_SLICING="$REPORT_DIR/slicing.txt"
REPORT_MONITORING="$REPORT_DIR/monitoring.txt"
SILENT=false
DEBUG=false
EXP_NAME="genadaptive"


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
            -n|-N|--name)
                shift
                [ ! -z "$1" ] && EXP_NAME=$1 || 
                echo "Invalid argument was provided: ${option}"; usage ; exit 1
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
    
cat << EOF
Usage: ${script_name} [OPTION]...
Runs adaptive monitoring experiments with MonPoly sequentially and without Flink.
Edit the topmost uppercase variables in order to change parameters of the experiments.
Version: ${script_version}
Options:
  -h|--help                 Displays this help
  -s|--silent               Displays no output
  -v|--verbose              Displays debug output
  -n|--name NAME            Sets the name of the experiment
EOF
}

function debug() {
    if [[ ${DEBUG} == "true" ]]; then
        echo "[DEBUG]" "$@"
    fi
}

function info() {
    if [[ ${SILENT} != "true" ]]; then
        echo "[INFO]" "$@"
    fi
}

function error() {
    if [[ ${SILENT} != "true" ]]; then
        echo "[ERROR]" "$@"
    fi
    exit 0
}

function make_log() {
    local formula=$1
    local er=$2
    local ir=$3
    local part=$4
    local strategy="$5"

    "$WORK_DIR/generator.sh" $formula -e $er -i $ir -x $VIOLATIONS -w $WINDOW -pA 0.3333 -pB 0.3333 -z "$strategy" $LOG_LENGTH > "$OUTPUT_DIR/${EXP_NAME}_${formula}_${er}_${ir}_part${part}"
    
    echo "${EXP_NAME}_${formula}_${er}_${ir}_part${part}"
}

function slice() {
    local formula=$1
    local num=$2
    local log=$3
    local output=$4

    "$WORK_DIR/slicer.sh" "$output" "$num" -formula "$formula"  -file "$log" 
}


function start_monpoly() {
    local formula=$1

    local fma=""
    case ${formula} in 
        -S) 
            fma="star.mfotl"
        ;;
        -T) 
            fma="triangle.mfotl"
        ;;
        -L)
            fma="linear.mfotl"
        ;;
        *) 
            error "Formula ${formula} does not exist"
        ;;
    esac

    
    debug "Creating pipes..."
    #create in pipe
    [ -p ${IN_PIPE} ] || error "Pipe ${IN_PIPE} already exists"
    mkfifo ${IN_PIPE}

    #create out pipe 
    [ -p ${OUT_PIPE} ] || error "Pipe ${OUT_PIPE} already exists"
    mkfifo ${OUT_PIPE}

    debug "Starting MonPoly..."
    #start background monpoly with the pipes
    "$WORK_DIR/monpoly" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" < ${IN_PIPE} > ${OUT_PIPE} &
    PID=$(echo $!)

}
function stop_monpoly() {
    debug "Stopping MonPoly"
    [ ! -z $PID ] || error "MonPoly is not running"
    kill $PID
    
    debug "Deleteing pipes"
    [ -p ${IN_PIPE} ] && rm ${IN_PIPE}
    [ -p ${OUT_PIPE} ] && rm ${OUT_PIPE}
}

function monitor() {
    local log=$1
    local report=$2
    local verdict=$3

    debug "Monitoring log ${log}"
    [ ! -p ${IN_PIPE} ] || error "Pipe ${IN_PIPE} does not exist"
    [ ! -p ${OUT_PIPE} ] || error "Pipe ${OUT_PIPE} does not exist"

    [ -z ${verdict} ] && verdict="/dev/null"
    cat $log > ${IN_PIPE}
    cat ${OUT_PIPE} > ${verdict}

}

function split() {
    local strategy=$1

    debug "Monitoring log ${log}"
    [ ! -p ${IN_PIPE} ] || error "Pipe ${IN_PIPE} does not exist"
    [ ! -p ${OUT_PIPE} ] || error "Pipe ${OUT_PIPE} does not exist"

    [ -z ${verdict} ] && verdict="/dev/null"
    cat $strategy > ${IN_PIPE} #TODO: reformat input
    cat ${OUT_PIPE} > ${verdict}
}

function merge() {

}


parse_options "$@"

info "=== Flinkless experiments ==="

# FOR each param
for procs in $PROCESSORS; do
    numcpus=${procs%/*}
    cpulist=${procs#*/}
    info "Number of processors: ${numcpus}"
    for f in $FORMULAS; do
        info "  Formula: ${f}"
        for er in $EVENT_RATES; do
            info "    Event rate: ${er}"
            for ir in $INDEX_RATES; do
                info "      Index rate: ${ir}"
                for ads in $NUM_ADAPTATIONS; do

                    adaptations=${ads%/*}
                    strategies=${ads#*/}
                    info "        Number of adaptations: ${adaptations}"
                    
                    first_strategy=$(echo $strategies | cut -d ";" -f 1)
                    for a in `seq 0 $adaptations`; do
                        
                        # Generate logs
                        debug "          Generating log part ${a} with strategy ${a}"
                        strategy=$(echo $strategies | cut -d ";" -f $((a+1)))
                        log=$(make_log "$f" "$er" "$ir" "$a" "$strategy") 
                        
                        # Slice the logs 
                        debug "          Slicing log part ${a} with strategy ${a}"
                        out="${log}_slice"
                        $TIME_COMMAND -f %e,%M -a -o $REPORT_SLICING slice "$f" "$num" "$log" "$out"
                        debug "          Slicing log part ${a} with strategy 0"
                        out="${log}_baseline_slice"
                        $TIME_COMMAND -f %e,%M -a -o $REPORT_SLICING slice "$f" "$num" "$log" "$out"
                    done
                    log="${EXP_NAME}_${f}_${er}_${ir}"
                    for r in $(seq 1 $REPETITIONS); do
                        info "            Running repetition ${r}..."
                        
                        info "            Baseline monitoring"
                        for slice in `seq 0 $((procs-1))`; do
                            start_monpoly $f 

                            for part in `seq 0 $adaptations`; do

                                # run monpoly
                                echo "      Monitoring log ${log}_part${part}_slice${slice}" >> $REPORT_MONITORING 
                                $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part${part}_slice${slice}"
                            
                            done

                            stop_monpoly 
                        done


                        info "            Adaptive monitoring"
                        for slice in `seq 0 $((procs-1))`; do
                            
                            start_monpoly $f 
                            
                            # run monpoly
                            echo "      Monitoring log ${log}_part0_slice${slice}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part0_slice${slice}"
                            
                            # split state
                            second_strategy=$(echo $strategies | cut -d ";" -f 2)
                            echo "      Splitting strategy ${second_strategy}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING split "$second_strategy"
                            
                            stop_monpoly 
                            
                            for part in `seq 1 $((adaptations-1))`; do
                                start_monpoly $f 

                                # merge state
                                strategy=$(echo $strategies | cut -d ";" -f $((part+1)))
                                echo "      Merging strategy ${strategy}" >> $REPORT_MONITORING 
                                $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING merge "$strategy"

                                # run monpoly
                                echo "      Monitoring log ${log}_part${part}_slice${slice}" >> $REPORT_MONITORING 
                                $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part${part}_slice${slice}"

                                # split state
                                strategy=$(echo $strategies | cut -d ";" -f $((part+2)))
                                echo "      Splitting strategy ${strategy}" >> $REPORT_MONITORING 
                                $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING split "$strategy"
                            
                                stop_monpoly 
                                
                            done

                            start_monpoly $f 

                            # merge state
                            strategy=$(echo $strategies | cut -d ";" -f $((adaptations+1)))
                            echo "      Merging strategy ${strategy}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING merge "$strategy"

                            # run monpoly
                            echo "      Monitoring log ${log}_part${adaptations}_slice${slice}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part${adaptations}_slice${slice}"

                            stop_monpoly 

                        done   
                    done 
                done
            done
        done
    done
done  