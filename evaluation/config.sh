[[ -n $WORK_DIR ]] || WORK_DIR=`cd "${BASH_SOURCE%/*}"; pwd`

export JAVA_HOME="$WORK_DIR/jdk1.8.0_172"

FLINK_BIN="$WORK_DIR/flink-1.5.0/bin"
MONITOR_JAR="$WORK_DIR/parallel-online-monitoring-1.0-SNAPSHOT.jar"
MONPOLY="$WORK_DIR/monpoly"

CHECKPOINT_DIR="$WORK_DIR/checkpoints"
OUTPUT_DIR="$WORK_DIR/output"

CPU_LIST=12-19
