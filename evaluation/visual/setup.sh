#!/usr/bin/env bash

# Assumption: the setup script is in the <git repo>/experiments/visual
# together with other important files. This script is called by overall
# setup script (i.e., $TARGET_DIR is the installation folder)
VISUAL_SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
TARGET_DIR=`pwd`
FLINK_HOME=${TARGET_DIR}/flink-1.5.0
VISUAL_TARGET_DIR=`pwd`/visual
mkdir -p ${VISUAL_TARGET_DIR}
cd ${VISUAL_TARGET_DIR}

echo "Updating evaluation files ..."
if ! rsync -a --exclude setup.sh "$VISUAL_SCRIPT_DIR/" . ; then
    echo "[ERROR] Could not update evaluation files."
    exit 1
fi


#SQLITE
if [[ ! -d sqlite ]]; then
	if ! curl -LR#O https://www.sqlite.org/2018/sqlite-tools-linux-x86-3240000.zip; then
		echo "Could not download sqlite"
		exit 1
	fi
	echo "Extracting sqlite..."
	unzip sqlite-tools-linux-x86-3240000.zip
	mv sqlite-tools-linux-x86-3240000 sqlite
	echo "PATH=\$PATH:`pwd`/sqlite" |  tee --append ~/.bash_profile
	source ~/.bash_profile
	rm sqlite-tools-linux-x86-3240000.zip
fi

#GRAFANA
if [[ ! -d grafana ]]; then
	if ! curl -LR#O https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana-5.1.3.linux-x64.tar.gz; then
		echo "Could not download Grafana"
		exit 1
	fi
	echo "Extracting Grafana..."
	tar -zxvf grafana-5.1.3.linux-x64.tar.gz
	mv grafana-5.1.3 grafana
	echo "Configuring Grafana..."
	cp grafana.ini grafana/conf/custom.ini
	rm grafana-5.1.3.linux-x64.tar.gz
fi

#Prometheus
if [[ ! -d prometheus ]]; then
	if ! curl -LR#O https://github.com/prometheus/prometheus/releases/download/v2.3.0/prometheus-2.3.0.linux-amd64.tar.gz; then
		echo "Could not download Prometheus"
		exit 1
	fi
	echo "Extracting Prometheus..."
	tar -xvf prometheus-2.3.0.linux-amd64.tar.gz
	mv prometheus-2.3.0.linux-amd64 prometheus
	echo "Configuring Prometheus..."
	cp prometheus.yml prometheus/
	echo "Configuring Flink..."
	echo "metrics.reporters: prom\nmetrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"
	cp $FLINK_HOME/opt/flink-metrics-prometheus-1.5.0.jar $FLINK_HOME/lib
	rm prometheus-2.3.0.linux-amd64.tar.gz
fi

cd ${TARGET_DIR}