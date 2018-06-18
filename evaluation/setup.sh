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
    JDK_ARCHIVE="server-jre-8u172-linux-x64.tar.gz"
    if [[ ! -f $JDK_ARCHIVE ]]; then
        echo "Downloading JRE ..."
        if ! curl -LR#O -H "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u172-b11/a58eab1ec242421181065cdc37240b08/server-jre-8u172-linux-x64.tar.gz; then
            echo "[ERROR] Could not download the JRE."
            exit 1
        fi
        echo "3d0a5db2300423a1fd6ee25c229dbd5320d79204c73844337f5b6a082d58541f  server-jre-8u172-linux-x64.tar.gz" > "$CHECKSUM_FILE"
        if ! sha256sum -c "$CHECKSUM_FILE"; then
            echo "[ERROR] File corrupted."
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
        if ! curl -LR#O http://www-eu.apache.org/dist/flink/flink-1.5.0/flink-1.5.0-bin-scala_2.11.tgz; then
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
if [[ (! -f $LDCC_SAMPLE) || ./nokia/cut_log.py -nt $LDCC_SAMPLE ]]; then
    echo "Cutting the LDCC log file. This may take some time ..."
    if ! ./nokia/cut_log.py < "$LDCC_LOG" > "$LDCC_SAMPLE"; then
        rm "$LDCC_SAMPLE"
        echo "[ERROR] Cutting failed."
        exit 1
    fi
fi


echo "Preparing working directories ..."
mkdir -p checkpoints
mkdir -p output
mkdir -p reports


export JAVA_HOME="$TARGET_DIR/jdk1.8.0_172"

DRIVER_JAR="parallel-online-monitoring-1.0-SNAPSHOT.jar"
TOOL_JAR="evaluation-tools-1.0-SNAPSHOT.jar"
MONPOLY_BIN="monpoly"
MISSING_FILE=0

if [[ ! -f $DRIVER_JAR ]]; then
    DRIVER_INSTALL="fail"
    echo "[WARNING] $DRIVER_JAR does not exist. Building..."
    if [[ ! -z $(which mvn) ]]; then
        `cd "${SCRIPT_DIR}"; cd ..; mvn package 2> /dev/null > /dev/null`
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

MONPOLY_DIR="mt-monpoly"
if [[ ! -x $MONPOLY_BIN ]]; then
    MONPOLY_INSTALL="fail"
    echo "[WARNING] $MONPOLY_BIN does not exist or is not executable. Compiling..."
    if [[ ! -d $MONPOLY_DIR ]]; then
        if [[ ! -z $(which git) ]]; then
            git clone -b slicer-integration https://bitbucket.org/FreddiB/mt-monpoly/
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
