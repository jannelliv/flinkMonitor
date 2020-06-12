#!/usr/bin/env bash

target_dir="$(pwd)"

info() {
    echo "[setup] $1"
}

fatal_error() {
    echo "[setup] ERROR: $1"
    exit 1
}

required_tools=("curl" "git" "opam" "mvn" "python3" "m4")
for tool in "${required_tools[@]}"; do
    which "$tool" > /dev/null || fatal_error "$tool is not installed"
done

### OCaml #####################################################################

ocaml_compiler="4.07.0"
export OPAMROOT="$target_dir/opam"
if [[ -d "$OPAMROOT" ]]; then
    info "opam environment exists, skipping"
    info "delete $OPAMROOT to reinstall"
else
    info "initialising OCaml environment"
    opam init --compiler="$ocaml_compiler" --no-setup || fatal_error "opam init failed"
fi
eval $(opam config env)
opam install -y ocamlfind || fatal_error "installing ocamlfind failed"

### MonPoly ###################################################################

monpoly_dir="$target_dir/monpoly"
monpoly_url="https://bitbucket.org/jshs/monpoly"
if [[ -d "$monpoly_dir" ]]; then
    info "monpoly directory exists, skipping"
    info "delete $monpoly_dir to reinstall"
    [[ -x "$monpoly_dir/monpoly" ]] || info "WARNING: monpoly executable not found, please build manually"
else
    info "cloning the monpoly repository"
    git clone "$monpoly_url" "$monpoly_dir" || fatal_error "could not clone the monpoly repository"
    (cd "$monpoly_dir" && make) || fatal_error "could not build monpoly"
fi

### Java ######################################################################

jdk_dir="$target_dir/jdk1.8.0_211"
jdk_url="https://bitbucket.org/krle/scalable-online-monitor/downloads/jdk-8u211-linux-x64.tar.gz"
if [[ -d "$jdk_dir" ]]; then
    info "JDK directory exists, skipping"
    info "delete $jdk_dir to reinstall"
else
    info "downloading the JDK"
    jdk_archive="$target_dir/jdk.tar.gz"
    [[ ! -a "$jdk_archive" ]] || fatal_error "would overwrite $jdk_archive"
    curl -fLR# -o "$jdk_archive" "$jdk_url" || fatal_error "could not download the JDK"
    (cd "$target_dir" && tar -xzf "$jdk_archive") || fatal_error "could not extract JDK archive"
    rm "$jdk_archive"
fi
export JAVA_HOME="$jdk_dir"


### Scala ######################################################################
scala_dir="$target_dir/scala-2.12.7"
scala_url="https://downloads.lightbend.com/scala/2.12.7/scala-2.12.7.tgz"
if [[ -d "$scala_dir" ]]; then
    info "Scala directory exists, skipping"
    info "delete $scala_dir to reinstall"
else
    info "downloading Scala"
    scala_archive="$target_dir/scala.tgz"
    [[ ! -a "$scala_archive" ]] || fatal_error "would overwrite $scala_archive"
    curl -fLR# -o "$scala_archive" "$scala_url" || fatal_error "could not download the JDK"
    (cd "$target_dir" && tar -xzf "$scala_archive") || fatal_error "could not extract JDK archive"
    rm "$scala_archive"
fi
export PATH="$PATH:${scala_dir}/bin"



### Parallel online monitor ###################################################

monitor_dir="$target_dir/scalable-online-monitor"
monitor_url="https://bitbucket.org/krle/scalable-online-monitor"
monitor_branch="multisource_checkpointing"
if [[ -d "$monitor_dir" ]]; then
    info "project directory exists, skipping"
    info "delete $monitor_dir to reinstall"
    [[ -a "$monitor_dir/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar" ]] || info "WARNING: monitor jar not found, please build manually"
    [[ -a "$monitor_dir/replayer/target/replayer-1.0-SNAPSHOT.jar" ]] || info "WARNING: replayer jar not found, please build manually"
    [[ -a "$monitor_dir/trace-generator/target/trace-generator-1.0-SNAPSHOT.jar" ]] || info "WARNING: trace generator jar not found, please build manually"
else
    info "cloning the project repository (initial branch: $monitor_branch)"
    git clone --branch "$monitor_branch" "$monitor_url" "$monitor_dir" || fatal_error "could not clone the project repository"
    info "building the project"
    (cd "$monitor_dir" && mvn clean -Dmaven.test.skip package) || fatal_error "could not build the scalable online monitor and related tools"
fi


### Flink #####################################################################

#TODO: add relaxed rescaling release for Flink 1.7
flink_dir="$target_dir/flink"
flink_url="https://archive.apache.org/dist/flink/flink-1.7.2/flink-1.7.2-bin-hadoop28-scala_2.12.tgz"
if [[ -d "$flink_dir" ]]; then
    info "Flink directory exists, skipping"
    info "delete $flink_dir to reinstall"
else
    info "downloading Flink"
    flink_archive="$target_dir/flink.tar.gz"
    [[ ! -a "$flink_archive" ]] || fatal_error "would overwrite flink.tar.gz"
    curl -fLR# -o "$flink_archive" "$flink_url" || fatal_error "could not download Flink"
    (cd "$target_dir" && tar -xzf "$flink_archive" && mv flink-1.7.2 flink) || fatal_error "could not extract Flink archive"
    rm "$flink_archive"
fi

kafka_dir="$target_dir/kafka"
kafka_url="https://archive.apache.org/dist/kafka/0.11.0.3/kafka_2.12-0.11.0.3.tgz"
if [[ -d "$kafka_dir" ]]; then
    info "Kafka directory exists, skipping"
    info "delete $kafka_dir to reinstall"
else
    info "downloading Kafka"
    kafka_archive="$target_dir/kafka.tar.gz"
    [[ ! -a "$kafka_archive" ]] || fatal_error "would overwrite kafka.tar.gz"
    curl -fLR# -o "$kafka_archive" "$kafka_url" || fatal_error "could not download Kafka"
    (cd "$target_dir" && tar -xzf "$kafka_archive" && mv kafka_2.12-0.11.0.3 kafka) || fatal_error "could not extract Kafka archive"
    rm "$kafka_archive"
fi

zookeeper_dir="$target_dir/zookeeper"
zookeeper_url="https://archive.apache.org/dist/zookeeper/zookeeper-3.5.6/apache-zookeeper-3.5.6-bin.tar.gz"
if [[ -d "$zookeeper_dir" ]]; then
    info "Zookeeper directory exists, skipping"
    info "delete $zookeeper_dir to reinstall"
else
    info "downloading Zookeeper"
    zookeeper_archive="$target_dir/zookeeper.tar.gz"
    [[ ! -a "$zookeeper_archive" ]] || fatal_error "would overwrite zookeeper.tar.gz"
    curl -fLR# -o "$zookeeper_archive" "$zookeeper_url" || fatal_error "could not download Zookeeper"
    (cd "$target_dir" && tar -xzf "$zookeeper_archive" && mv apache-zookeeper-3.5.6-bin zookeeper) || fatal_error "could not extract Zookeeper archive"
    rm "$zookeeper_archive"
fi

# Configuration
info "replacing the Flink configuration"
cp "$monitor_dir/evaluation/flink-conf.yaml" "$flink_dir/conf" || fatal_error "could not copy the Flink configuration"

info "replacing the Kafka configuration"
cp "$monitor_dir/evaluation/server.properties" "$kafka_dir/config" || fatal_error "could not copy the Kafka configuration"

if [[ -a "$zookeeper_dir/conf/zoo.cfg" ]]; then
    info "zookeeper config already exists"
else
    info "replacing the Zookeeper configuration"
    mv "$zookeeper_dir/conf/zoo_sample.cfg" "$zookeeper_dir/conf/zoo.cfg"
fi

### Nokia log #################################################################

ldcc_file="$target_dir/ldcc.csv"
ldcc_url="http://sourceforge.net/projects/monpoly/files/ldcc.tar"
if [[ -a "$ldcc_file" ]]; then
    info "nokia log file exists, skipping"
else
    info "downloading the nokia log file"
    ldcc_archive="$target_dir/ldcc.tar"
    [[ ! -a "$ldcc_archive" ]] || fatal_error "would overwrite ldcc.tar"
    [[ ! -a "$ldcc_file.gz" ]] || fatal_error "would overwrite $ldcc_file.gz"
    curl -fLR# -o "$ldcc_archive" "$ldcc_url" || fatal_error "could not download the nokia log"
    info "extracting the nokia log file"
    (tar -xOf "$ldcc_archive" ldcc.csv.gz > "$ldcc_file.gz" && gunzip "$ldcc_file.gz") || fatal_error "could not extract the nokia log"
    rm "$ldcc_archive"
    chmod a-w "$ldcc_file"
fi

ldcc_sample="$target_dir/ldcc_sample.csv"
ldcc_sample_past="$target_dir/ldcc_sample_past.csv"
if [[ (! -a "$ldcc_sample") || (! -a "$ldcc_sample_past") || "$monitor_dir/evaluation/nokia/cut_log.py" -nt "$ldcc_sample" ]]; then
    info "cutting the Nokia log file"
    rm -f "$ldcc_sample" "$ldcc_sample_past"
    if ! "$monitor_dir/evaluation/nokia/cut_log.py" "$ldcc_file" "$ldcc_sample_past" "$ldcc_sample"; then
        rm -f "$ldcc_sample" "$ldcc_sample_past"
        fatal_error "could not cut the Nokia log"
    fi
fi

### Working and output directories ############################################

info "creating working directories"
mkdir -p checkpoints
mkdir -p output
mkdir -p reports
mkdir -p logs
mkdir -p state

###############################################################################

info "done!"
