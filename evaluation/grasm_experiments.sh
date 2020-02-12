#!/bin/bash
pkill -9 java

./nokia/experiments_kafka_overhead.sh
mkdir nokia_kafka_overhead_reports
cp -a reports/* nokia_kafka_overhead_reports
pkill -9 java

./nokia/experiments_socket_overhead.sh
mkdir nokia_socket_overhead_reports
cp -a reports/* nokia_socket_overhead_reports
pkill -9 java

./nokia/experiments_socket_no_overhead.sh
mkdir nokia_socket_no_overhead_reports
cp -a reports/* nokia_socket_no_overhead_reports
pkill -9 java

./synthetic/experiments_socket_imbalance.sh
mkdir socket_imbalance_reports
cp -a reports/* socket_imbalance_reports
pkill -9 java

./synthetic/experiments_socket_mode4.sh
mkdir socket_mode4_reports
cp -a reports/* socket_mode4_reports
pkill -9 java

./synthetic/experiments_socket.sh
mkdir socket_reports
cp -a reports/* socket_reports
pkill -9 java

./nokia/experiments_socket.sh
mkdir nokia_socket_reports
cp -a reports/* nokia_socket_reports
pkill -9 java
