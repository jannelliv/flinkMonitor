#!/usr/bin/env bash

# Assumption: the setup script is in the <git repo>/experiments
# together with other important files. 
SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
TARGET_DIR=`pwd`

read -p "Install the evaluation components to $TARGET_DIR? (Y/n) " -s -n 1 choice
echo
if [[ $choice = "n" || $choice = "N" ]]; then exit 1; fi


echo "Updating evaluation files ..."
if ! rsync -a --exclude setup.sh "$SCRIPT_DIR/" . ; then
    echo "[ERROR] Could not update evaluation files."
    exit 1
fi


echo "Installing missing components"

CHECKSUM_FILE="checksum.tmp"

JDK_DIR="jdk1.8.0_181"
if [[ ! -d $JDK_DIR ]]; then
    JDK_ARCHIVE="server-jre-8u181-linux-x64.tar.gz"
    if [[ ! -f $JDK_ARCHIVE ]]; then
        echo "Downloading JRE ..."
        if ! curl -LR#O -H "Cookie: oraclelicense=accept-securebackup-cookie" download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/${JDK_ARCHIVE}; then
            echo "[ERROR] Could not download the JRE."
            exit 1
        fi
    fi
    echo "Extracting JRE ..."
    if ! tar -xzf "$JDK_ARCHIVE"; then
        echo "[ERROR] Could not extract the JRE."
        exit 1
    fi
fi

#SCALA_DIR="scala-2.11.8"
#if [[ ! -d $SCALA_DIR ]]; then
#    SCALA_ARCHIVE="scala-2.11.8.tgz"
#    if [[ ! -f $SCALA_ARCHIVE ]]; then
#        echo "Downloading Scala ..."
#        if ! curl -LR#O downloads.lightbend.com/scala/2.11.8/${SCALA_ARCHIVE}; then
#            echo "[ERROR] Could not download scala."
#            exit 1
#        fi
#    fi
#    echo "Extracting scala ..."
#    if ! tar -xzf "$SCALA_ARCHIVE"; then
#        echo "[ERROR] Could not extract scala."
#        exit 1
#    fi
#fi

export JAVA_HOME="$TARGET_DIR/$JDK_DIR"
#export SCALA_HOME="$TARGET_DIR/$SCALA_DIR"
echo $JAVA_HOME
#echo $SCALA_HOME

FLINK_DIR="flink-1.5.0"
FLINK_DIR_SRC="flink-1.5.0-src"
if [[ ! -d ${FLINK_DIR} ]]; then
    FLINK_ARCHIVE="flink-1.5.0-src.tgz"
    if [[ ! -f ${FLINK_ARCHIVE} ]]; then
        echo "Downloading Flink ..."
        if ! curl -LR#O archive.apache.org/dist/flink/flink-1.5.0/${FLINK_ARCHIVE}; then
            echo "[ERROR] Could not download Flink."
            exit 1
        fi
    fi

    if [[ ! -f ${FLINK_DIR_SRC} ]]; then
        mkdir -p ${FLINK_DIR_SRC}
        echo "Extracting Flink ..."
        if ! tar -xzf "$FLINK_ARCHIVE" -C ${FLINK_DIR_SRC} --strip-components 1; then
            echo "[ERROR] Could not extract Flink."
            rm -r ${FLINK_DIR_SRC}
            rm ${FLINK_ARCHIVE}
            exit 1
        fi
    fi


    FLINK_EDIT_FOLDER="flink-edit"
    FLINK_EDIT_FILE="RescalingHandlers.java"
    FLINK_EDIT_PATH="flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/job/rescaling"

    if [[ ! -d "$FLINK_EDIT_FOLDER" || ! -f "$FLINK_EDIT_FOLDER/$FLINK_EDIT_FILE" ]]; then
        echo "Edited Flink file does not exist"
        exit 1
    fi

    echo "Copying modified Flink file"
    if ! cp "$FLINK_EDIT_FOLDER/$FLINK_EDIT_FILE" "$FLINK_DIR_SRC/$FLINK_EDIT_PATH"; then
        echo "File could not be copied to location: $FLINK_EDIT_PATH"
        exit 1
    fi

    echo "Building from source"
    if ! (cd ${FLINK_DIR_SRC} && mvn clean install -DskipTests -Dfast > /dev/null); then
        echo "Building Flink from source failed"
        exit 1
    fi
    if ! (cd ${FLINK_DIR_SRC}/flink-dist && mvn clean install > /dev/null); then
        echo "Building Flink from source failed"
        exit 1
    fi

    if ! mv ${FLINK_DIR_SRC}/flink-dist/target/flink-1.5.0-bin/flink-1.5.0 $FLINK_DIR; then
        echo "Moving built flink to working dir failed"
        exit 1
    fi
fi

LDCC_LOG="ldcc.csv"
if [[ ! -f $LDCC_LOG ]]; then
    LDCC_ARCHIVE="ldcc.tar"
    if [[ ! -f $LDCC_ARCHIVE ]]; then
        echo "Downloading the LDCC (Nokia) log file ..."
        if ! curl -LR#O http://sourceforge.net/projects/monpoly/files/ldcc.tar; then
            echo "[ERROR] Could not download the LDCC log file."
            exit 1
        fi
    fi
    echo "Extracting the LDCC (Nokia) log file ..."
    if ! (tar -xf "$LDCC_ARCHIVE" "$LDCC_LOG.gz" && gunzip "$LDCC_LOG.gz"); then
        echo "[ERROR] Could not extract the LDCC log file."
        exit 1
    fi
    echo "34d794778dbbc0652b409a41be994c726678d3d123f887703acf47d0002903b5  ldcc.csv" > "$CHECKSUM_FILE"
    if ! sha256sum -c "$CHECKSUM_FILE"; then
        echo "[ERROR] File corrupted."
        exit 1
    fi
    chmod a-w "$LDCC_LOG"
    rm "$LDCC_ARCHIVE"
fi

if [[ -f $CHECKSUM_FILE ]]; then
    rm "$CHECKSUM_FILE"
fi


echo "Installing the Flink configuration ..."
if ! cp flink-conf.yaml "$FLINK_DIR/conf"; then
    echo "[ERROR] Could not install the Flink configuration."
    exit 1
fi

LDCC_SAMPLE="ldcc_sample.csv"
LDCC_SAMPLE_PAST="ldcc_sample_past.csv"
if [[ (! -f $LDCC_SAMPLE) || (! -f $LDCC_SAMPLE_PAST) || ./nokia/cut_log.py -nt $LDCC_SAMPLE ]]; then
    echo "Cutting the LDCC log file. This may take some time ..."
    if ! ./nokia/cut_log.py "$LDCC_LOG" "$LDCC_SAMPLE_PAST" "$LDCC_SAMPLE"; then
        rm "$LDCC_SAMPLE" "$LDCC_SAMPLE_PAST"
        echo "[ERROR] Cutting failed."
        exit 1
    fi
fi


RATES="rates-trace.csv"
HEAVY="heavy-trace.csv"
COMPUTED_STATISTICS="ldcc_statistics.csv"
if [[ (-f ${COMPUTED_STATISTICS}) ]]; then
    if [[ ((! -f $HEAVY) || (! -f $RATES)) ]]; then
        HEAVY_RAW="heavy_raw.csv"
        RATES_RAW="rates_raw.csv"

        echo "Splitting statistics"
        if ! ./nokia/split_statistics.py ${COMPUTED_STATISTICS} ${RATES_RAW} ${HEAVY_RAW}; then
            rm "$HEAVY_RAW" "$RATES_RAW"
            exit 1
        fi

        sed '1d' $RATES_RAW > $RATES
        sed '1d' $HEAVY_RAW > $HEAVY

        rm $HEAVY_RAW
        rm $RATES_RAW
    fi
else
    echo "Computed statistics missing"
    exit -1
fi


echo "Preparing working directories ..."
mkdir -p checkpoints
mkdir -p output
mkdir -p reports


DRIVER_JAR="parallel-online-monitoring-1.0-SNAPSHOT.jar"
TOOL_JAR="evaluation-tools-1.0-SNAPSHOT.jar"
PROXY_JAR="ReplayerProxy-assembly-0.1.0-SNAPSHOT.jar"
MONPOLY_BIN="monpoly"
MISSING_FILE=0

if [[ ! -f $DRIVER_JAR ]]; then
    DRIVER_INSTALL="fail"
    echo "[WARNING] $DRIVER_JAR does not exist. Building..."
    if [[ ! -z $(which mvn) ]]; then
        `cd "${SCRIPT_DIR}"; cd ..; mvn -P build-jar package 2> /dev/null > /dev/null`
        if [[ $? -eq 0 ]]; then
            if  cp ${SCRIPT_DIR}/../target/parallel-online-monitoring-1.0-SNAPSHOT.jar .; then
                DRIVER_INSTALL="success"
                echo "$DRIVER_JAR installed from source"
            fi
        fi
    fi
    if [[ $DRIVER_INSTALL = "fail" ]]; then
            echo "Cannot build jar locally. Please copy the parallel online monitor jar into the current directory."
            MISSING_FILE=1
    fi
fi

if [[ ! -f $TOOL_JAR ]]; then
    TOOL_INSTALL="fail"
    echo "[WARNING] $TOOL_JAR does not exist. Building..."
    if [[ ! -z $(which mvn) ]]; then
        `cd "${SCRIPT_DIR}"; cd ../tools; mvn package 2> /dev/null > /dev/null`
        if [[ $? -eq 0 ]]; then
            if  cp ${SCRIPT_DIR}/../tools/target/evaluation-tools-1.0-SNAPSHOT.jar .; then
                TOOL_INSTALL="success"
                echo "$TOOL_JAR installed from source"
            fi
        fi
    fi
    if [[ $TOOL_INSTALL = "fail" ]]; then
            echo "Cannot build jar locally. Please copy the evaluation tool jar into the current directory."
            MISSING_FILE=1
    fi
fi

PROXY_DIR="mt-csv-proxy"
if [[ ! -f $PROXY_JAR ]]; then
    PROXY_INSTALL="fail"
    echo "[WARNING] $PROXY_JAR does not exist. Building..."
    if [[ ! -z $(which mvn) ]]; then
        if [[ ! -z $(which git) ]]; then
            git clone https://bitbucket.org/FreddiB/mt-csv-proxy/
        fi
        `cd "${PROXY_DIR}"; sbt assembly 2> /dev/null > /dev/null`
        if [[ $? -eq 0 ]]; then
            if  cp ${PROXY_DIR}/target/scala-2.10/ReplayerProxy-assembly-0.1.0-SNAPSHOT.jar .; then
                PROXY_INSTALL="success"
                echo "$PROXY_JAR installed from source"
            fi
        fi
    fi
    if [[ $PROXY_INSTALL = "fail" ]]; then
            echo "Cannot build jar locally. Please copy the evaluation proxy jar into the current directory."
            MISSING_FILE=1
    fi
fi

MONPOLY_DIR="mt-monpoly"
if [[ ! -x $MONPOLY_BIN ]]; then
    MONPOLY_INSTALL="fail"
    echo "[WARNING] $MONPOLY_BIN does not exist or is not executable. Compiling..."
    if [[ ! -d $MONPOLY_DIR ]]; then
        if [[ ! -z $(which git) ]]; then
            git clone https://bitbucket.org/FreddiB/mt-monpoly/
        fi
    fi
    if [[ ! -z $(which opam) ]]; then
        `cd "${MONPOLY_DIR}"; make 2> /dev/null > /dev/null`
        if [[ $? -eq 0 ]]; then
            if  cp ${MONPOLY_DIR}/monpoly .; then
                MONPOLY_INSTALL="success"
                echo "$MONPOLY_BIN installed from source"
            fi
        fi
    fi

    if [[ $MONPOLY_INSTALL = "fail" ]]; then
        echo "Please copy the Monpoly binary into the current directory."
        MISSING_FILE=1
    fi
fi

chmod +x "$SCRIPT_DIR/nokia/cut_log.py"
chmod +x "$SCRIPT_DIR/nokia/split_statistics.py"
chmod +x "$SCRIPT_DIR/monitor.sh"
chmod +x "$SCRIPT_DIR/proxy.sh"
chmod +x "$SCRIPT_DIR/replayer.sh"

[[ $MISSING_FILE = 0 ]] || exit 1

echo
echo "Evaluation uses the following programs:"
sha256sum "$DRIVER_JAR" "$MONPOLY_BIN"
