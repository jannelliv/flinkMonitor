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

JDK_DIR="jdk1.8.0_172"
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

FLINK_DIR="flink-1.5.0"
if [[ ! -d $FLINK_DIR ]]; then
    FLINK_ARCHIVE="flink-1.5.0-bin-scala_2.11.tgz"
    if [[ ! -f $FLINK_ARCHIVE ]]; then
        echo "Downloading Flink ..."
        if ! curl -LR#O https://archive.apache.org/dist/flink/flink-1.5.0/flink-1.5.0-bin-scala_2.11.tgz; then
            echo "[ERROR] Could not download Flink."
            exit 1
        fi
        echo "0664149acc2facb7193160e1e0c1cec571300f2c22adeb01e2e605871cda2d47c9dab16731c7168f389f5b0eff9e7484ff6206898bc13c3be8b9d0027d36d14f  flink-1.5.0-bin-scala_2.11.tgz" > "$CHECKSUM_FILE"
        if ! sha512sum -c "$CHECKSUM_FILE"; then
            echo "[ERROR] File corrupted."
            exit 1
        fi
    fi
    echo "Extracting Flink ..."
    if ! tar -xzf "$FLINK_ARCHIVE"; then
        echo "[ERROR] Could not extract Flink."
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
        if ! ./nokia/split_statistics.py ${COMPUTED_STATISTICS} ${HEAVY_RAW} ${RATES_RAW}; then
            rm "$HEAVY_RAW" "$RATES_RAW"
            exit 1
        fi

        sed -i '1d' $RATES_RAW > $RATES


        HEAVY="heavy.csv"
        echo "Merging heavy"
        if ! cat ${HEAVY_RAW} | ./nokia/merge_heavy.py > ${HEAVY}; then
            rm "$HEAVY_RAW" "$RATES_RAW" "$HEAVY" "$RATES"
            exit 1
        fi
        rm $HEAVY_RAW
    fi
else
    echo "Computed statistics missing"
    exit -1
fi


echo "Preparing working directories ..."
mkdir -p checkpoints
mkdir -p output
mkdir -p reports

export JAVA_HOME="$TARGET_DIR/jdk1.8.0_181"


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

[[ $MISSING_FILE = 0 ]] || exit 1

echo
echo "Evaluation uses the following programs:"
sha256sum "$DRIVER_JAR" "$MONPOLY_BIN"
