#!/usr/bin/env bash

target_dir="$(pwd)"

info() {
    echo "[setup] $1"
}

fatal_error() {
    echo "[setup] ERROR: $1"
    exit 1
}

required_tools=("curl" "git" "opam" "mvn" "python3")
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

jdk_dir="$target_dir/jdk1.8.0_192"
jdk_url="https://download.oracle.com/otn-pub/java/jdk/8u192-b12/750e1c8617c5452694857ad95c3ee230/jdk-8u192-linux-x64.tar.gz"
if [[ -d "$jdk_dir" ]]; then
    info "JDK directory exists, skipping"
    info "delete $jdk_dir to reinstall"
else
    info "downloading the JDK"
    jdk_archive="$target_dir/jdk.tar.gz"
    [[ ! -a "$jdk_archive" ]] || fatal_error "would overwrite $jdk_archive"
    curl -fLR# -H "Cookie: oraclelicense=accept-securebackup-cookie" -o "$jdk_archive" "$jdk_url" || fatal_error "could not download the JDK"
    (cd "$target_dir" && tar -xzf "$jdk_archive") || fatal_error "could not extract JDK archive"
    rm "$jdk_archive"
fi
export JAVA_HOME="$jdk_dir"

### Parallel online monitor ###################################################

monitor_dir="$target_dir/scalable-online-monitor"
monitor_url="https://bitbucket.org/krle/scalable-online-monitor"
monitor_branch="autobalancer"
if [[ -d "$monitor_dir" ]]; then
    info "project directory exists, skipping"
    info "delete $monitor_dir to reinstall"
    [[ -a "$monitor_dir/target/parallel-online-monitoring-1.0-SNAPSHOT.jar" ]] || info "WARNING: monitor jar not found, please build manually"
    [[ -a "$monitor_dir/tools/target/evaluation-tools-1.0-SNAPSHOT.jar" ]] || info "WARNING: tool jar not found, please build manually"
else
    info "cloning the project repository (initial branch: $monitor_branch)"
    git clone --branch "$monitor_branch" "$monitor_url" "$monitor_dir" || fatal_error "could not clone the project repository"
    info "building the project"
    (cd "$monitor_dir" && mvn package -P build-jar) || fatal_error "could not build the scalable online monitor"
    (cd "$monitor_dir/tools" && mvn package) || fatal_error "could not build the evaluation tools"
fi

### Flink #####################################################################

flink_dir="$target_dir/flink"
flink_url="https://github.com/jshs/flink/releases/download/relaxed-rescaling-1.5.0/flink-1.5-relaxed-rescaling.tar.gz"
if [[ -d "$flink_dir" ]]; then
    info "Flink directory exists, skipping"
    info "delete $flink_dir to reinstall"
else
    info "downloading Flink"
    flink_archive="$target_dir/flink.tar.gz"
    [[ ! -a "$flink_archive" ]] || fatal_error "would overwrite flink.tar.gz"
    curl -fLR# -o "$flink_archive" "$flink_url" || fatal_error "could not download Flink"
    (cd "$target_dir" && tar -xzf "$flink_archive") || fatal_error "could not extract Flink archive"
    rm "$flink_archive"
fi

# Configuration
info "replacing the Flink configuration"
cp "$monitor_dir/evaluation/flink-conf.yaml" "$flink_dir/conf" || fatal_error "could not copy the Flink configuration"

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
    rm "$ldcc_sample" "$ldcc_sample_past"
    if ! "$monitor_dir/evaluation/nokia/cut_log.py" "$ldcc_file" "$ldcc_sample_past" "$ldcc_sample"; then
        rm "$ldcc_sample" "$ldcc_sample_past"
        fatal_error "could not cut the Nokia log"
    fi
fi

### Working and output directories ############################################

info "creating working directories"
mkdir -p checkpoints
mkdir -p output
mkdir -p reports

###############################################################################

info "done!"
