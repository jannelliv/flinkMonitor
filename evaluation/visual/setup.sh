#!/bin/bash

#SQLITE
if [[ ! -d sqlite ]]; then
	wget https://www.sqlite.org/2018/sqlite-tools-linux-x86-3240000.zip
	unzip sqlite-tools-linux-x86-3240000.zip
	mv sqlite-tools-linux-x86-3240000 sqlite
	echo "PATH=\$PATH:`pwd`/sqlite" |  tee --append ~/.bash_profile
	source ~/.bash_profile
	rm sqlite-tools-linux-x86-3240000.zip
fi

#GRAFANA
if [[ ! -d grafana ]]; then
	wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana-5.1.3.linux-x64.tar.gz 
	tar -zxvf grafana-5.1.3.linux-x64.tar.gz
	mv grafana-5.1.3 grafana
	cp grafana.ini grafana/conf/custom.ini
	rm grafana-5.1.3.linux-x64.tar.gz
fi

#Prometheus
if [[ ! -d prometheus ]]; then
	wget https://github.com/prometheus/prometheus/releases/download/v2.3.0/prometheus-2.3.0.linux-amd64.tar.gz
	tar -xvf prometheus-2.3.0.linux-amd64.tar.gz
	mv prometheus-2.3.0.linux-amd64 prometheus
	cp prometheus.yml prometheus/
	echo "metrics.reporters: prom\nmetrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"
	cp $FLINK_HOME/opt/flink-metrics-prometheus-1.5.0.jar $FLINK_HOME/lib
	rm prometheus-2.3.0.linux-amd64.tar.gz
fi
