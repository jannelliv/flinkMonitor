#!/usr/bin/env bash


# PREAMBLE
WORK_DIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
source "$WORK_DIR/config.sh"


# ============================================================
# OUTPUT functions
# ============================================================

SILENT=false
DEBUG=false

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

# ============================================================
# Script parameters 
# ============================================================

EXP_NAME="genadaptive"
SKIP_GENERATE=true
SKIP_MONITOR=true


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
                SKIP_MONITOR=false
                if [[ $# -ge 6 ]]; then
                    f=$1                ; shift #formula
                    er=$1               ; shift #event rate
                    ir=$1               ; shift #index rate
                    numcpus=$1          ; shift #number of parallel cpus
                    NUM_ADAPTATIONS=$1  ; shift #all adaptaions 
                    REPETITIONS=$1      ; shift #number of repetitions
                else
                    echo "Invalid argument was provided: ${option}"
                    usage
                    exit 1
                fi
                ;;
            -g|-G|--generate)
                SKIP_GENERATE=false
                if [[ $# -ge 7 ]]; then
                    f=$1                ; shift #formula
                    er=$1               ; shift #event rate
                    ir=$1               ; shift #index rate
                    NUM_ADAPTATIONS=$1  ; shift #all adaptaions 
                    PROCESSORS=$1       ; shift #all parallelisms
                    LOG_LENGTH=$1       ; shift #log lenght
                    WINDOW=$1           ; shift #lenght of the window
                else
                    echo "Invalid argument was provided: ${option}"
                    usage
                    exit 1
                fi
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
  -h|--help              Displays this help
  -s|--silent            Displays no output
  -v|--verbose           Displays debug output
  -n|--name NAME         Sets the name of the experiment
  -m|--monitor PARAMS    Runs the monitor with parameters 
        PARAMS = formula event_rate index_rate numcpus all_adaptations num_repetitions
  -g|--generate          Runs log generation with parameters 
        PARAMS = formula event_rate index_rate all_adaptations all_parallelisms log_length violations window
EOF
}

# ============================================================
# Output formatting
# ============================================================

declare -A TIMEMAP


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


    local key="${part}, ${rep}"

    # debug "Putting $time in TIMEMAP at (${key})"

    local result=${TIMEMAP[$key]}

    # debug "Current TIMEMAP[(${key})]=${result}"

    
    if [ -z "$result" ]; then
        TIMEMAP[$key]="$time"
    else 
        TIMEMAP[$key]="${result}, $time"
    fi

    local result=${TIMEMAP[$key]}

    # debug "New TIMEMAP[(${key})]=${result}"


}

function write_times (){
    local file=$1
    local numcpus=$2

    local header="Part, Repetition, $(eval echo  "Baseline{0..$((numcpus-1))},")"
    local header=${header%?}
    for slice in `seq 0 $((numcpus-1))`; do
        header="${header}, Merge${slice}, Monitor${slice}, Split${slice}"
    done 
    echo ${header} > $file

    for K in "${!TIMEMAP[@]}"; do
        echo "${K}, ${TIMEMAP[$K]}" >> $file
    done

}


# ============================================================
# Log generation functions
# ============================================================


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
    "$WORK_DIR/generator.sh" $formula -e $er -i $ir -t $start -w $WINDOW $strategy $length | "$WORK_DIR/replayer.sh" -a 0 -m > $log 

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

# ============================================================
# Monitoring functions
# ============================================================
IN_PIPE="/tmp/in$$"
OUT_PIPE="/tmp/out$$"
MONPOLY=$ROOT_DIR/monpoly/monpoly


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
    
    [ ! -p ${IN_PIPE} ] && error "Pipe ${IN_PIPE} does not exist"
    [ ! -p ${OUT_PIPE} ] && error "Pipe ${OUT_PIPE} does not exist"


    local ts1=$(date +%s%N)
    
    cat $log > ${IN_PIPE}
    echo ">get_pos<" > ${IN_PIPE}

    local loaded=""
    while [[ "$loaded" != *"Current timepoint"* ]]; do 
        read loaded < ${OUT_PIPE}
    done
    debug "Finished monitoring (${loaded})"

    local ts2=$(date +%s%N)
    local delta=$((ts2 - ts1))
    local time=`bc <<< "scale=2; $delta/1000000000"`
    
    #returns time
    echo $time
    # $TIME_COMMAND -f %e,%M -a -o $REPORT_MONITORING cat $log > ${IN_PIPE}
}

# records and returns time 
function split() {
    local strategy=$1
    local state=$2
    local result=$3

    if [ ! -z "$strategy" ]; then

        # debug "Splitting state according to the strategy ${strategy}"
        [ ! -p ${IN_PIPE} ] && error "Pipe ${IN_PIPE} does not exist"
        [ ! -p ${OUT_PIPE} ] && error "Pipe ${OUT_PIPE} does not exist"

        local payload1=">set_slicer $(cat $strategy)<"
        local payload2=">split_save ${state}<"

        local ts1=$(date +%s%N)

        echo "$payload1" > ${IN_PIPE}
        echo "$payload2" > ${IN_PIPE}

    fi

    # debug "Stopping MonPoly"
    [ ! -z $PID ] || error "MonPoly is not running"
    exec 3>&-
    wait $PID 2> /dev/null
    exec 4<&-

    if [ ! -z "$strategy" ]; then

        local ts2=$(date +%s%N)
        local delta=$((ts2 - ts1))
        local time=`bc <<< "scale=2; $delta/1000000000"`

        #returns time
        eval $result="$time"
    fi
    
    # debug "Deleteing pipes"
    [ -p ${IN_PIPE} ] && rm -f ${IN_PIPE}
    [ -p ${OUT_PIPE} ] && rm -f ${OUT_PIPE}
}

# records and returns time 
function merge() {
    local formula=$1
    local state=$2
    local result=$3

    
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

        #start background monpoly with the pipes
        "$MONPOLY" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" -nonewlastts -nofilteremptytp < ${IN_PIPE} > ${OUT_PIPE} &
        PID=$(echo $!)
        exec 3> ${IN_PIPE}
        exec 4< ${OUT_PIPE}

    else 
        # debug "Merging states ${state}"

        #start background monpoly with the pipes and state

        local ts1=$(date +%s%N)
        local state=${state%?}
        local state=${state//[[:blank:]]/}
        # debug "Running" "$MONPOLY" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" -nonewlastts -nofilteremptytp -combine "${state}" 
        "$MONPOLY" -negate -sig "$WORK_DIR/flinkless/synth.sig" -formula "$WORK_DIR/flinkless/$fma" -nonewlastts -nofilteremptytp -combine "${state}" < ${IN_PIPE} > ${OUT_PIPE} &
        PID=$(echo $!)
        exec 3> ${IN_PIPE}
        exec 4< ${OUT_PIPE}
        
        local loaded=""
        while [[ "$loaded" != *"Loaded"* ]]; do 
            read loaded < ${OUT_PIPE}
        done

        debug "$loaded (and merged)"

        local ts2=$(date +%s%N)
        local delta=$((ts2 - ts1))
        local time=`bc <<< "scale=2; $delta/1000000000"`

        #returns time
        eval $result="$time" 

    fi
}



# ============================================================
# Script start
# ============================================================

parse_options "$@"


if [[ ${SKIP_GENERATE} == "false" ]]; then

    TIFS=$IFS
    info "  Generating logs for the formula ${f})"
    info "    Generating logs with event rates ${er})"
    info "      Generating logs with index rates ${ir})"  
    export IFS="#"      
    for ads in $NUM_ADAPTATIONS; do

        tmp=${ads%/*}
        adaptations=${tmp%/*}
        num=${tmp#*/}
        
        tmp=${ads#*/}
        strategies=${tmp#*/}

        length=$(( LOG_LENGTH/(adaptations+1) ))
        info "        Generating logs for strategy ${num}"          
        export IFS=$TIFS
        for a in `seq 0 $adaptations`; do

            # Generate logs
            info "          Generating log part ${a} (out of {0..${adaptations}})"
            strategy=$(echo $strategies | cut -d ";" -f $((a+1)))
            log=$(make_log "$f" "$er" "$ir" "$a" "$num" "$length" "$strategy")
            info "            Slicing..."
            for procs in $PROCESSORS; do
                numcpus=${procs%/*}
                cpulist=${procs#*/}
                debug "            Slicing log part ${a} into ${numcpus} slices"
                # Slice the logs 
                # optimally
                debug "              Slicing log part ${a} with strategy ${a}"
                in=$(log_path "${log}")
                out=$(log_path "${log}_${numcpus}_slice")
                slice "$f" "$numcpus" "$in" "$out"

                # baseline
                debug "              Slicing log part ${a} with strategy 0 (baseline)"
                out="$OUTPUT_DIR/${log}_${numcpus}_baseline_slice"
                name=$(log_name "$num" "$f" "$er" "$ir" 0)
                log0=$(log_path $name)
                strategy="$(cat ${log0}_${numcpus}_slice_strategy)"
                slice "$f" "$numcpus" "$in" "$out" "$strategy"

            done
        done
    done
fi

if [[ ${SKIP_MONITOR} == "false" ]]; then

    TIFS=$IFS
    info "Number of processors ${numcpus}"
    info "  Formula ${f}"
    info "    Event rate: ${er}"
    info "      Index rate: ${ir}"
    export IFS="#"
    for ads in $NUM_ADAPTATIONS; do
        tmp=${ads%/*}
        adaptations=${tmp%/*}
        num=${tmp#*/}
        tmp=${ads#*/}
        strategies=${tmp#*/}
        info "        Strategy: ${num}"
        
        export IFS=$TIFS
        info "          Baseline monitoring"
        for slice in `seq 0 $((numcpus-1))`; do
            for r in $(seq 1 $REPETITIONS); do
            
                start_monpoly $f 

                for part in `seq 0 $adaptations`; do
                    info "            Running (repetition: ${r}, part: ${part}, slice: ${slice})"
                    
                    name=$(log_baseline_name "$num" "$f" "$er" "$ir" "$part" "$slice" "$numcpus")
                    log=$(log_path $name)
                    time=$(monitor "${log}")
                    add_time $part $r $time
                done

                stop_monpoly

            done
        done

        info "          Adaptive monitoring"

        #for fixed part = 0
        for slice in `seq 0 $((numcpus-1))`; do

            for r in $(seq 1 $REPETITIONS); do

                start_monpoly "$f"

                # run monpoly
                info "            Running (repetition: ${r}, part: 0, slice: ${slice})"
                name=$(log_name "$num" "$f" "$er" "$ir" "0" "$slice" "$numcpus")
                log=$(log_path $name)
                debug "Monitoring ${log}"
                time=$(monitor "${log}")
                add_time "0" $r "- , $time" # - stands for no preceeding merge 
                
                # split state (also stops monpoly)
                name=$(log_name "$num" "$f" "$er" "$ir" "1")
                log=$(log_path $name)
                debug "Splitting state according to the strategy ${log}_${numcpus}_slice_strategy"
                runtime=0
                split "${log}_${numcpus}_slice_strategy" "${CHECKPOINT_DIR}/${name}_${numcpus}_slice${slice}_state" runtime
                add_time "0" $r "$runtime" 

            done              
        done

        for part in `seq 1 $((adaptations-1))`; do
        
            for slice in `seq 0 $((numcpus-1))`; do

                for r in $(seq 1 $REPETITIONS); do

                    # merge state (also starts monpoly)
                    log=$(log_name "$num" "$f" "$er" "$ir" "$part")
                    state=$(eval echo "${CHECKPOINT_DIR}/${log}_${numcpus}_slice{0..$((numcpus-1))}_state-${slice}.bin,")
                    debug "Merging states ${state}"
                    runtime=0
                    merge "$f" "$state" runtime
                    add_time $part $r $runtime
                    
                    # run monpoly
                    info "            Running (repetition: ${r}, part: ${part}, slice: ${slice})"
                    name=$(log_name "$num" "$f" "$er" "$ir" "$part" "$slice" "$numcpus")
                    log=$(log_path $name)
                    debug "Monitoring ${log}"
                    time=$(monitor "${log}")
                    add_time $part $r $time

                    # split state (also stops monpoly)
                    name=$(log_name "$num" "$f" "$er" "$ir" $((part+1)))
                    log=$(log_path $name)
                    debug "Splitting state according to the strategy ${log}_${numcpus}_slice_strategy"
                    runtime=0
                    split "${log}_${numcpus}_slice_strategy" "${CHECKPOINT_DIR}/${name}_${numcpus}_slice${slice}_state" runtime
                    add_time $part $r $runtime
                
                done 
        
            done

            #cleaning up the states
            for slice in `seq 0 $((numcpus-1))`; do
                log=$(log_name "$num" "$f" "$er" "$ir" "$part")
                states=$(eval echo "${CHECKPOINT_DIR}/${log}_${numcpus}_slice{0..$((numcpus-1))}_state-${slice}.bin ")
                rm $states
            done
                   

        done

        #for fixed part = $adaptations
        for slice in `seq 0 $((numcpus-1))`; do

            for r in $(seq 1 $REPETITIONS); do

                # merge state (also starts monpoly)
                log=$(log_name "$num" "$f" "$er" "$ir" "$adaptations")
                state=$(eval echo "${CHECKPOINT_DIR}/${log}_${numcpus}_slice{0..$((numcpus-1))}_state-${slice}.bin,")
                debug "Merging states ${state}"
                runtime=0
                merge "$f" "$state" runtime
                add_time $adaptations $r $runtime

                # run monpoly
                info "            Running (repetition: ${r}, part: ${adaptations}, slice: ${slice})"
                name=$(log_name "$num" "$f" "$er" "$ir" "$adaptations" "$slice" "$numcpus")
                log=$(log_path $name)
                debug "Monitoring ${log}"
                time=$(monitor "${log}")
                add_time $adaptations $r "$time, - " # - stands for no subsequent split

                stop_monpoly 
            
            done 
        done

        report="${REPORT_DIR}/$(report_name ${f} ${er} ${ir} ${num} ${numcpus})"
        write_times $report $numcpus
        unset TIMEMAP
        declare -A TIMEMAP
    done


fi

