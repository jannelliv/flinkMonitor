#!/usr/bin/env bash

# Experiments that run MonPoly without Flink

# EXPERIMENT PARAMETERS:
REPETITIONS=3
FORMULAS="-S -L -T"
EVENT_RATES="2000 2500 3000 3500 4000 5000 6000 8000"
INDEX_RATES="1000"
LOG_LENGTH="60"
# HEAVY_SETS_NO_STATS="h0 h1"
# HEAVY_SETS_STATS="h1"
PROCESSORS="4/0-5,24-29 8/0-9,24-33 16/0-8,12-20,24-32,36-44"
NUM_ADAPTATIONS='1/ ;-pA 0.01 -pB 0.495#1/ ;-z "x=10+1000,y=0,z=0,w=0"#1/-z "x=10+1000,y=0,z=0,w=0";#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+2000,y=0,z=0,w=0"#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=2+1000,y=0,z=0,w=0"#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=0,y=10+1000,z=0,w=0"#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+1000,y=10+2000,z=0,w=0"' 
WINDOW=10
VIOLATIONS=0.01


# Log generation strategies 
# Equal relation ratios and uniform value distribution (i.e., -pA 0.3333 -pB 0.3333 -z "x=0,y=0,z=0,w=0")
# Params                                                      |       Explanation
# ----------------------------------------------------------------------------------------------------------
#1/ ;-pA 0.01 -pB 0.495                                        |       change relation rates
#1/ ;-z "x=10+1000,y=0,z=0,w=0"                                |       introduce a single HH value 
#1/-z "x=10+1000,y=0,z=0,w=0";                                 |       remove a single HH value 
#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+2000,y=0,z=0,w=0"       |       change a single HH value
#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=2+1000,y=0,z=0,w=0"        |       change the number of HH values
#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=0,y=10+1000,z=0,w=0"       |       change the HH variable
#1/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+1000,y=10+2000,z=0,w=0" |       change the number of HH variables


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



function log_name() {
    local adaptations=$1
    local formula=$2
    local er=$3
    local ir=$4
    local part=$5
    local slice=$6

    local name="${EXP_NAME}_${adaptations}_${formula}_${er}_${ir}_part${part}"

    if [ -z "$slice" ]; then 
        echo $name
    else
        echo "${name}_slice${slice}"
    fi
}

function log_baseline_name() {
    local adaptations=$1
    local formula=$2
    local er=$3
    local ir=$4
    local part=$5
    local slice=$6

    local name="${EXP_NAME}_${adaptations}_${formula}_${er}_${ir}_part${part}"

    if [ -z "$slice" ]; then 
        echo $name
    else
        echo "${name}_baseline_slice${slice}"
    fi
}


function log_strategy() {
    local log=$1
    echo "${log}_slice_strategy"
}

function log_path() {
    local log=$1
    echo "$OUTPUT_DIR/${log}"
}

function state_name(){
    local log=$1
    echo "${log}_state"
}

function state_path() {
    local state=$1
    echo "$CHECKPOINT_DIR/${state}"
}

function make_log() {
    local formula=$1
    local er=$2
    local ir=$3
    local part=$4
    local strategy="$5"
    local adaptations=$6
    local length=$7

    

    local name=$(log_name "$adaptations" "$formula" "$er" "$ir" "$part")
    local log=$(log_path $name)

    "$WORK_DIR/generator.sh" $formula -e $er -i $ir -x $VIOLATIONS -w $WINDOW "$strategy" $length | "$WORK_DIR/replayer.sh" -a 0 -m > $log 
    
    echo "${name}"
}

function slice() {
    local formula=$1
    local num=$2
    local log=$3
    local output=$4
    local strategy=$5

    if [ -z "$strategy" ]; then 
        "$WORK_DIR/slicer.sh" "$output" "$num" -formula "$formula"  -file "$log"
    else
        "$WORK_DIR/slicer.sh" "$output" "$num" -formula "$formula"  -file "$log" -slicer "$strategy"
    fi 

}


function start_monpoly() {
    local formula=$1

    merge $1
}
function stop_monpoly() {
    debug "Stopping MonPoly"
    [ ! -z $PID ] || error "MonPoly is not running"
    kill $PID
    exec 3>&-
    
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
    local state=$2

    debug "Splitting state according to the strategy ${strategy}"
    [ ! -p ${IN_PIPE} ] || error "Pipe ${IN_PIPE} does not exist"
    [ ! -p ${OUT_PIPE} ] || error "Pipe ${OUT_PIPE} does not exist"

    [ -z ${verdict} ] && verdict="/dev/null"

    local payload=">set_slicer $(cat $strategy)<"
    echo $payload > ${IN_PIPE} 
    local payload=">split_save ${state}<"
    echo $payload > ${IN_PIPE} 
    cat ${OUT_PIPE} > ${verdict}
}

function merge() {
    local formula=$1
    local state=$2

    
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

    if [ -z "${state}" ]; then

        #start background monpoly with the pipes
        "$WORK_DIR/monpoly" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" < ${IN_PIPE} > ${OUT_PIPE} &
        exec 3> ${IN_PIPE}
        PID=$(echo $!)

    else 
        debug "Merging states ${state}"

        #start background monpoly with the pipes and state
        "$WORK_DIR/monpoly" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" -combine "${state}" < ${IN_PIPE} > ${OUT_PIPE} &
        exec 3> ${IN_PIPE}
        PID=$(echo $!)

    fi
        


}



parse_options "$@"

info "=== Generating logs ==="

#TODO: use a flag for this

for f in $FORMULAS; do
    for er in $EVENT_RATES; do
        for ir in $INDEX_RATES; do
            export IFS="#"
            for ads in $NUM_ADAPTATIONS; do

                adaptations=${ads%/*}
                strategies=${ads#*/}
                                
                length=$((LOG_LENGTH/ads))
                export IFS=" "
                for a in `seq 0 $adaptations`; do

                    # Generate logs
                    debug "          Generating log part ${a} (out of ${ads})"
                    strategy=$(echo $strategies | cut -d ";" -f $((a+1)))
                    log=$(make_log "$f" "$er" "$ir" "$a" "$strategy" "$a" "$length")

                    # Slice the logs 
                    # optimally
                    debug "          Slicing log part ${a} with strategy ${a}"
                    in=$(log_path "${log}")
                    out=$(log_path "${log}_slice")
                    $TIME_COMMAND -f %e,%M -a -o $REPORT_SLICING slice "$f" "$num" "$in" "$out"

                    # baseline
                    debug "          Slicing log part ${a} with strategy 0"
                    out="$OUTPUT_DIR/${log}_baseline_slice"
                    name=$(log_name "$adaptations" "$formula" "$er" "$ir" 0)
                    log0=$(log_path $name)
                    strategy="$(cat ${log0}_slice_strategy)"
                    $TIME_COMMAND -f %e,%M -a -o $REPORT_SLICING slice "$f" "$num" "$in" "$out" "$strategy"

                done
            done
        done
    done
done 

info "=== Running flinkless experiments ==="


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
                    info "        Number of adaptations: ${adaptations}"
                    
                    log="$OUTPUT_DIR/${EXP_NAME}_${f}_${er}_${ir}"
 
                    info "          Baseline monitoring"
                    for slice in `seq 0 $((procs-1))`; do
                        start_monpoly $f 
                        for part in `seq 0 $adaptations`; do
                            for r in $(seq 1 $REPETITIONS); do
                                info "            Running (${slice}, ${part}, ${r})..."
                                # run monpoly
                                echo "      Monitoring log ${log}_part${part}_baseline_slice${slice}" >> $REPORT_MONITORING 
                                $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part${part}_baseline_slice${slice}"
                            done
                        done
                        stop_monpoly 
                    done

                    info "          Adaptive monitoring"

                    #for fixed part = 0
                    for slice in `seq 0 $((procs-1))`; do

                        start_monpoly "$f"
                        
                        # run monpoly
                        echo "      Monitoring log ${log}_part0_slice${slice}" >> $REPORT_MONITORING 
                        $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part0_slice${slice}"
                        
                        # split state
                        echo "      Splitting strategy ${log}_part1_slice_strategy" >> $REPORT_MONITORING 
                        $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING split "${log}_part1_slice_strategy" "${CHECKPOINT_DIR}/${log}_part1_slice${slice}_state"
                        
                        stop_monpoly 
                
                    done

                    for part in `seq 1 $((adaptations-1))`; do
                    
                        for slice in `seq 0 $((procs-1))`; do

                            # start_monpoly $f 

                            # merge state (also starts monpoly)
                            strategy=$(echo $strategies | cut -d ";" -f $((part+1)))
                            echo "      Merging strategy ${strategy}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING merge "$strategy"

                            state=$(eval echo "${CHECKPOINT_DIR}/${log}_part${part}_slice{0..$((procs-1))}_state-${slice}.bin")
                            echo "      Merging strategy ${state}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING merge "$f" "$state"

                            # run monpoly
                            echo "      Monitoring log ${log}_part${part}_slice${slice}" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING monitor "${log}_part${part}_slice${slice}"

                            echo "      Splitting strategy ${log}_part$((part+1))_slice_strategy" >> $REPORT_MONITORING 
                            $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING split "${log}_part$((part+1))_slice_strategy" "${CHECKPOINT_DIR}/${log}_part$((part+1))_slice${slice}_state"
                        
                            stop_monpoly 
                    
                        done

                    done

                    #for fixed part = $adaptations
                    for slice in `seq 0 $((procs-1))`; do

                        # start_monpoly "$f" 

                        # merge state (also starts monpoly)
                        state=$(eval echo "${CHECKPOINT_DIR}/${log}_part${adaptations}_slice{0..$((procs-1))}_state-${slice}.bin")
                        echo "      Merging strategy ${state}" >> $REPORT_MONITORING 
                        $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING merge "$f" "$state"

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