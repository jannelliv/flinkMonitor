#!/bin/bash
pkill -9 java

./nokia/experiments_nokia3.sh
mkdir nokia3_reports
cp -a ../../reports/* nokia3_reports
rm -rf ../../reports/*
pkill -9 java

./nokia/experiments_nokia2.sh
mkdir nokia2_reports
cp -a ../../reports/* nokia2_reports
rm -rf ../../reports/*
pkill -9 java

./nokia/experiments_nokia3.sh
mkdir nokia1_reports
cp -a ../../reports/* nokia1_reports
rm -rf ../../reports/*
pkill -9 java

#./synthetic/experiments_socket_imbalance.sh
#mkdir socket_imbalance_reports
#cp -a ../../reports/* socket_imbalance_reports
#rm -rf ../../reports/*
#pkill -9 java

#./synthetic/experiments_socket_mode4.sh
#mkdir socket_mode4_reports
#cp -a ../../reports/* socket_mode4_reports
#rm -rf ../../reports/*
#pkill -9 java

#./synthetic/experiments_socket.sh
#mkdir socket_reports
#cp -a ../../reports/* socket_reports
#rm -rf ../../reports/*
#pkill -9 java


