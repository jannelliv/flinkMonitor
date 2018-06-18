[[ -n $WORK_DIR ]] || WORK_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`

export JAVA_HOME="$WORK_DIR/jdk1.8.0_172"
export PATH="$PATH:$WORK_DIR/visual/sqlite"

FLINK_BIN="$WORK_DIR/flink-1.5.0/bin"
MONITOR_JAR="$WORK_DIR/parallel-online-monitoring-1.0-SNAPSHOT.jar"
TOOL_JAR="$WORK_DIR/evaluation-tools-1.0-SNAPSHOT.jar"

CHECKPOINT_DIR="$WORK_DIR/checkpoints"
OUTPUT_DIR="$WORK_DIR/output"
REPORT_DIR="$WORK_DIR/reports"

CPU_LIST=12-19
AUX_CPU_LIST=20-23

TIME_COMMAND=/usr/bin/time
