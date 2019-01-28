#!/usr/bin/env bash

# Experiments that run MonPoly without Flink

# EXPERIMENT PARAMETERS:
REPETITIONS=1
FORMULAS="-S -L -T"
EVENT_RATES="500" #"2000 2500 3000 3500 4000 5000 6000 8000"
INDEX_RATES="1"
LOG_LENGTH="1000"
# HEAVY_SETS_NO_STATS="h0 h1"
# HEAVY_SETS_STATS="h1"
PROCESSORS="4/0-5,24-29 8/0-9,24-33" # 16/0-8,12-20,24-32,36-44"
NUM_ADAPTATIONS='1/1/ ;-pA 0.01 -pB 0.495#1/2/ ;-z "x=10+1000,y=0,z=0,w=0"#1/3/-z "x=10+1000,y=0,z=0,w=0";' #1/4/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+2000,y=0,z=0,w=0"#1/5/-z "x=10+1000,y=0,z=0,w=0";-z "x=2+1000,y=0,z=0,w=0"#1/6/-z "x=10+1000,y=0,z=0,w=0";-z "x=0,y=10+1000,z=0,w=0"#1/7/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+1000,y=10+2000,z=0,w=0"' 
WINDOW=10
VIOLATIONS=0.1


# Log generation strategies 
# Equal relation ratios and uniform value distribution (i.e., -pA 0.3333 -pB 0.3333 -z "x=0,y=0,z=0,w=0")
# Params                                                         |       Explanation
# num_adapt/ID/strategies                                        |
# ----------------------------------------------------------------------------------------------------------
#1/1/ ;-pA 0.01 -pB 0.495                                        |       change relation rates
#1/2/ ;-z "x=10+1000,y=0,z=0,w=0"                                |       introduce a single HH value 
#1/3/-z "x=10+1000,y=0,z=0,w=0";                                 |       remove a single HH value 
#1/4/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+2000,y=0,z=0,w=0"       |       change a single HH value
#1/5/-z "x=10+1000,y=0,z=0,w=0";-z "x=2+1000,y=0,z=0,w=0"        |       change the number of HH values
#1/6/-z "x=10+1000,y=0,z=0,w=0";-z "x=0,y=10+1000,z=0,w=0"       |       change the HH variable
#1/7/-z "x=10+1000,y=0,z=0,w=0";-z "x=10+1000,y=10+2000,z=0,w=0" |       change the number of HH variables


# PREAMBLE & PRIVATE PARAMETERS
WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"

IN_PIPE="/tmp/in"
OUT_PIPE="/tmp/out"
REPORT_SLICING="$REPORT_DIR/slicing.txt"
REPORT_MONITORING="$REPORT_DIR/monitoring.txt"
MONPOLY=$ROOT_DIR/monpoly/monpoly
SILENT=false
DEBUG=false
EXP_NAME="genadaptive"
SKIP_GENERATE=false
SKIP_MONITOR=false
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
  -m|--monitor              Skips generating the logs (runs the monitor assuming that logs exist)
  -g|--generate             Skips monitoring the logs (runs only log the generation)
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

function report_name() {
    local formula=$1
    local er=$2
    local ir=$3
    local strategyID=$4
    local numcpus=$5

    if [ -z "$numcpus" ]; then
        echo "${EXP_NAME}_${formula}_${er}_${ir}_${strategyID}"
    else
        echo "${EXP_NAME}_${formula}_${er}_${ir}_${strategyID}_${numcpus}"
    fi
}

function add_time (){
    local part=$1
    local rep=$2
    local time=$3

    local result=${TIMEMAP[$part]}

    local key="${part}, ${rep}"
    if [ -z $result ]; then
        TIMEMAP[$key]="$time"
    else 
        TIMEMAP[$key]="${result}, $time"
    fi
    
}

function write_times (){
    local file=$1
    local numcpus=$2

    echo -n "Part, Repetition, $(eval echo  "Baseline{0..$((numcpus-2))},") Baseline$((numcpus-1)), " > $file 
    echo "Monitor0, Split, $(eval echo "Merge,\ Monitor{1..$((numcpus-2))},\ Split,") Merge, Monitor$((numcpus-1))" >> $file

    for K in "${!TIMEMAP[@]}"; do
        echo "${K}, ${TIMEMAP[$K]}" >> $file
    done

    unset TIMEMAP
    declare -A TIMEMAP
}

function add_slice_time (){
    local part=$1
    local time=$2

    local result=${TIMEMAP[$part]}

    local key="${part}"
    if [ -z $result ]; then
        TIMEMAP[$key]="$time"
    else 
        TIMEMAP[$key]="${result}, $time"
    fi
    
}

function write_slice_times (){
    local file=$1

    local header="Part, "
    for procs in $PROCESSORS; do
        numcpus=${procs%/*}
        header="${header}, ${numcpus}cpus"
    done 
                          
    echo $header > $file

    for K in "${!TIMEMAP[@]}"; do
        echo "${K}, ${TIMEMAP[$K]}" >> $file
    done

    unset TIMEMAP
    declare -A TIMEMAP
}


function log_name() {
    local adaptations=$1
    local formula=$2
    local er=$3
    local ir=$4
    local part=$5
    local slice=$6
    local numcpus=$7

    local name="${EXP_NAME}_${adaptations}_${formula}_${er}_${ir}_part${part}"

    if [ -z "$slice" ]; then 
        echo $name
    else
        echo "${name}_${numcpus}_slice${slice}"
    fi
}

function log_baseline_name() {
    local adaptations=$1
    local formula=$2
    local er=$3
    local ir=$4
    local part=$5
    local slice=$6
    local numcpus=$7

    local name="${EXP_NAME}_${adaptations}_${formula}_${er}_${ir}_part${part}"

    if [ -z "$slice" ]; then 
        echo $name
    else
        echo "${name}_${numcpus}_baseline_slice${slice}"
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
    local adaptations=$5
    local length=$6
    local strategy=$7

    local start=$((length*part))

    local name=$(log_name "$adaptations" "$formula" "$er" "$ir" "$part")
    local log=$(log_path $name)

    strategy=$(echo $strategy | sed s/\"//g)
    "$WORK_DIR/generator.sh" $formula -e $er -i $ir -t $start -x $VIOLATIONS -w $WINDOW $strategy $length | "$WORK_DIR/replayer.sh" -a 0 -m > $log 

    echo "${name}"
}

function slice() {
    local formula=$1
    local num=$2
    local log=$3
    local output=$4
    local strategy=$5

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

    if [ -z "$strategy" ]; then 
        "$WORK_DIR/slicer.sh" "$output" "$num" -formula "$WORK_DIR/flinkless/$fma"  -file "$log"
    else
        "$WORK_DIR/slicer.sh" "$output" "$num" -formula "$WORK_DIR/flinkless/$fma"  -file "$log" -slicer "$strategy"
    fi 

}


function start_monpoly() {
    local formula=$1

    merge $1
}
function stop_monpoly() {
    split 
}

# records and returns time 
function monitor() {
    local log=$1
    local report=$2
    local verdict=$3

    [ ! -p ${IN_PIPE} ] && error "Pipe ${IN_PIPE} does not exist"
    [ ! -p ${OUT_PIPE} ] && error "Pipe ${OUT_PIPE} does not exist"

    [ -z ${verdict} ] && verdict="/dev/null"

    local time=$((( $TIME_COMMAND -f '%e' cat $log > ${IN_PIPE}; ) 1> ${verdict}; ) 2>&1; )
    
    #returns time
    echo $time
    # $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING cat $log > ${IN_PIPE}
}

# records and returns time 
function split() {
    local strategy=$1
    local state=$2

    if [ ! -z "$strategy" ]; then

        # debug "Splitting state according to the strategy ${strategy}"
        [ ! -p ${IN_PIPE} ] && error "Pipe ${IN_PIPE} does not exist"
        [ ! -p ${OUT_PIPE} ] && error "Pipe ${OUT_PIPE} does not exist"

        [ -z ${verdict} ] && verdict="/dev/null"

        local payload1=">set_slicer $(cat $strategy)<"
        local payload2=">split_save ${state}<"

        local ts1=$(date +%s%N)

        echo "$payload1" > ${IN_PIPE}
        echo "$payload2" > ${IN_PIPE}

    fi

    sleep 1
    # debug "Stopping MonPoly"
    [ ! -z $PID ] || error "MonPoly is not running"
    exec 3>&-
    wait $PID 2> /dev/null

    if [ ! -z "$strategy" ]; then

        local ts2=$(date +%s%N)
        local delta=$((ts2 - ts1))
        local time=`bc <<< "scale=2; $delta/1000000000"`

        #returns time
        echo $time 
    fi
    
    # debug "Deleteing pipes"
    [ -p ${IN_PIPE} ] && rm -f ${IN_PIPE}
    [ -p ${OUT_PIPE} ] && rm -f ${OUT_PIPE}
}

# records and returns time 
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

    
    # debug "Creating pipes..."
    #create in pipe
    [ -p ${IN_PIPE} ] && error "Pipe ${IN_PIPE} already exists"
    mkfifo ${IN_PIPE}

    #create out pipe 
    [ -p ${OUT_PIPE} ] && error "Pipe ${OUT_PIPE} already exists"
    mkfifo ${OUT_PIPE}

    # debug "Starting MonPoly..."

    if [ -z "${state}" ]; then

        echo 
        #start background monpoly with the pipes
        "$MONPOLY" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" -nonewlastts -nofilteremptytp < ${IN_PIPE} > /dev/null &
        PID=$(echo $!)
        exec 3> ${IN_PIPE}
        

    else 
        # debug "Merging states ${state}"

        #start background monpoly with the pipes and state

        local ts1=$(date +%s%N)

        local state=${state%?}
        local state=${state//[[:blank:]]/}
        "$MONPOLY" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" -nonewlastts -nofilteremptytp -combine "${state}" < ${IN_PIPE} > /dev/null &
        PID=$(echo $!)
        exec 3> ${IN_PIPE}

        local ts2=$(date +%s%N)
        local delta=$((ts2 - ts1))
        local time=`bc <<< "scale=2; $delta/1000000000"`

        #returns time
        echo $time 
        

    fi
        


}



parse_options "$@"

if [[ ${SKIP_GENERATE} == "false" ]]; then

    info "=== Generating logs ==="
    rm -f $OUTPUT_DIR/*

    #TODO: use a flag for this
    TIFS=$IFS
    for f in $FORMULAS; do
        debug "  Generating logs for the formula ${f})"
        for er in $EVENT_RATES; do
            debug "    Generating logs with event rates ${er})"
            for ir in $INDEX_RATES; do
                export IFS="#"
                debug "      Generating logs with index rates ${ir})"
                for ads in $NUM_ADAPTATIONS; do

                    tmp=${ads%/*}
                    adaptations=${tmp%/*}
                    num=${tmp#*/}
                    
                    tmp=${ads#*/}
                    strategies=${tmp#*/}

                    length=$(( LOG_LENGTH/(adaptations+1) ))
                    debug "        Generating logs for strategy ${num}"          
                    export IFS=$TIFS
                    for a in `seq 0 $adaptations`; do

                        # Generate logs
                        debug "          Generating log part ${a} (out of {0..${adaptations}})"
                        strategy=$(echo $strategies | cut -d ";" -f $((a+1)))
                        log=$(make_log "$f" "$er" "$ir" "$a" "$num" "$length" "$strategy")

                        for procs in $PROCESSORS; do
                            numcpus=${procs%/*}
                            cpulist=${procs#*/}
                            debug "          Slicing log part ${a} into ${numcpus} slices"
                            # Slice the logs 
                            # optimally
                            debug "            Slicing log part ${a} with strategy ${a}"
                            in=$(log_path "${log}")
                            out=$(log_path "${log}_${numcpus}_slice")
                            slice "$f" "$numcpus" "$in" "$out"

                            # baseline
                            debug "            Slicing log part ${a} with strategy 0 (baseline)"
                            out="$OUTPUT_DIR/${log}_${numcpus}_baseline_slice"
                            name=$(log_name "$adaptations" "$f" "$er" "$ir" 0)
                            log0=$(log_path $name)
                            strategy="$(cat ${log0}_${numcpus}_slice_strategy)"
                            slice "$f" "$numcpus" "$in" "$out" "$strategy"

                        done

                        # report="${REPORT_DIR}/$(report_name ${f} ${er} ${ir} ${num})_slice"
                        # write_slice_times $report
                    done
                done
            done
        done
    done 
fi


if [[ ${SKIP_MONITOR} == "false" ]]; then

info "=== Running flinkless experiments ==="

    rm -f ${IN_PIPE}
    rm -f ${OUT_PIPE}
    rm -f $REPORT_DIR/*
    rm -f $CHECKPOINT_DIR/*

    # FOR each param
    TIFS=$IFS
    for procs in $PROCESSORS; do
        numcpus=${procs%/*}
        cpulist=${procs#*/}
        info "Number of processors ${numcpus} (out of $PROCESSORS)"
        for f in $FORMULAS; do
            info "  Formula ${f} (out of $FORMULAS)"
            for er in $EVENT_RATES; do
                info "    Event rate: ${er} (out of $EVENT_RATES)"
                for ir in $INDEX_RATES; do
                    info "      Index rate: ${ir} (out of $INDEX_RATES)"
                    export IFS="#"
                    for ads in $NUM_ADAPTATIONS; do
                        tmp=${ads%/*}
                        adaptations=${tmp%/*}
                        num=${tmp#*/}
                        tmp=${ads#*/}
                        strategies=${tmp#*/}
                        info "        Strategy: ${num}"
                        
                        export IFS=$TIFS
                        # info "          Baseline monitoring"
                        # for slice in `seq 0 $((numcpus-1))`; do
                        #     for r in $(seq 1 $REPETITIONS); do
                            
                        #         start_monpoly $f 

                        #         for part in `seq 0 $adaptations`; do
                        #             info "            Running (repetition: ${r}, part: ${part}, slice: ${slice})"
                                    
                        #             name=$(log_baseline_name "$adaptations" "$f" "$er" "$ir" "$part" "$slice" "$numcpus")
                        #             log=$(log_path $name)
                        #             time=$(monitor "${log}")
                        #             add_time $part $r $time
                        #         done

                        #         stop_monpoly

                        #     done
                        # done

                        info "          Adaptive monitoring"

                        #for fixed part = 0
                        for slice in `seq 0 $((numcpus-1))`; do

                            for r in $(seq 1 $REPETITIONS); do

                                start_monpoly "$f"

                                # run monpoly
                                info "            Running (repetition: ${r}, part: 0, slice: ${slice})"
                                name=$(log_name "$adaptations" "$f" "$er" "$ir" "0" "$slice" "$numcpus")
                                log=$(log_path $name)
                                debug "Monitoring ${log}"
                                time=$(monitor "${log}")
                                add_time "0" $r $time
                                
                                # split state (also stops monpoly)
                                name=$(log_name "$adaptations" "$f" "$er" "$ir" "1")
                                log=$(log_path $name)
                                debug "Splitting state according to the strategy ${log}_${numcpus}_slice_strategy"
                                time=$(split "${log}_${numcpus}_slice_strategy" "${CHECKPOINT_DIR}/${name}_${numcpus}_slice${slice}_state")
                                add_time "0" $r $time

                            done              
                        done

                        for part in `seq 1 $((adaptations-1))`; do
                        
                            for slice in `seq 0 $((numcpus-1))`; do

                                for r in $(seq 1 $REPETITIONS); do

                                    # merge state (also starts monpoly)
                                    log=$(log_name "$adaptations" "$f" "$er" "$ir" "$part")
                                    state=$(eval echo "${CHECKPOINT_DIR}/${log}_${numcpus}_slice{0..$((numcpus-1))}_state-${slice}.bin,")
                                    debug "Merging states ${state}"
                                    time=$(merge "$f" "$state")
                                    add_time $part $r $time

                                    # run monpoly
                                    info "            Running (repetition: ${r}, part: ${part}, slice: ${slice})"
                                    name=$(log_name "$adaptations" "$f" "$er" "$ir" "$part" "$slice" "$numcpus")
                                    log=$(log_path $name)
                                    debug "Monitoring ${log}"
                                    time=$(monitor "${log}")
                                    add_time $part $r $time

                                    # split state (also stops monpoly)
                                    name=$(log_name "$adaptations" "$f" "$er" "$ir" $((part+1)))
                                    log=$(log_path $name)
                                    debug "Splitting state according to the strategy ${log}_${numcpus}_slice_strategy"
                                    time=$(split "${log}_${numcpus}_slice_strategy" "${CHECKPOINT_DIR}/${name}_${numcpus}_slice${slice}_state")
                                    add_time $part $r $time
                                
                                done 
                        
                            done

                        done

                        #for fixed part = $adaptations
                        for slice in `seq 0 $((numcpus-1))`; do

                            for r in $(seq 1 $REPETITIONS); do

                                # merge state (also starts monpoly)
                                log=$(log_name "$adaptations" "$f" "$er" "$ir" "$adaptations")
                                state=$(eval echo "${CHECKPOINT_DIR}/${log}_${numcpus}_slice{0..$((numcpus-1))}_state-${slice}.bin,")
                                debug "Merging states ${state}"
                                time=$(merge "$f" "$state")
                                add_time $adaptations $r $time

                                # run monpoly
                                info "            Running (repetition: ${r}, part: ${adaptations}, slice: ${slice})"
                                name=$(log_name "$adaptations" "$f" "$er" "$ir" "$adaptations" "$slice" "$numcpus")
                                log=$(log_path $name)
                                debug "Monitoring ${log}"
                                time=$(monitor "${log}")
                                add_time $adaptations $r $time

                                stop_monpoly 
                            
                            done 
                        done

                        report="${REPORT_DIR}/$(report_name ${f} ${er} ${ir} ${num} ${numcpus})"
                        write_times $report $numcpus
                    done
                done
            done
        done
    done  

fi