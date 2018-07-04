#!/usr/bin/env bash

# Example usage 
# ./scrape.sh <start utc timestamp> <end utc timestamp> <output file prefix>
#./scrape.sh 2018-06-24T09:00:00.000Z 2018-06-25T10:00:00.000Z synthetic
# creates files synthetic_max.json, synthetic_average.json, and synthetic_peak.json

# The script performs multiple 3 hour range queries, since the time resolution of
# query answers has a hardcoded bound of 11k data points.

SCRIPT_DIR=`cd "$(dirname "$BASH_SOURCE")"; pwd`
TARGET_DIR=`pwd`


format="+%Y-%m-%dT%H:%M:%S.%3NZ"

start="$1"
end="$2"
output="$TARGET_DIR/$3"

start_utc=$(echo "$start" | xargs -I J date "$format" --utc -d J)
end_utc=$(echo "$end" | xargs -I J date "$format" --utc -d J)

start_s=$(echo "$start" | xargs -I J date '+%s'  -d J)
end_s=$(echo "$end" | xargs -I J date '+%s'  -d J)

diff_s=$((end_s-start_s))

#number of 3-hour queries 
periods=$((diff_s/10800))
mod=$( expr $diff_s % 10800)

[[ mod -eq 0 ]] || periods=$((periods+1))

metrics="max average peak"

for m in $metrics; do
    echo "{ \"all\": [ " > ${output}_${m}.json
done

echo "{ \"all\": [ " > ${output}_records.json

for i in `seq 1 $periods`; do

  end_utc=$(echo $start_utc | xargs -I J date $format --utc -d "J+3hours")
  # scrape
  echo "Scraping from $start_utc to $end_utc"
  #latency metrics
  for m in $metrics; do
    url="http://localhost:9090/api/v1/query_range?query=flink_taskmanager_job_task_operator_latency_"${m}"&start="${start_utc}"&end="${end_utc}"&step=1s"
    curl $url 2> /dev/null >> ${output}_${m}.json
    if [[ ! $i -eq $periods ]]; then 
      echo "," >> ${output}_${m}.json
    fi
  done

  #number of records metrics
  url="http://localhost:9090/api/v1/query_range?query=flink_taskmanager_job_task_operator_numEvents&start="${start_utc}"&end="${end_utc}"&step=1s"
  curl $url 2> /dev/null >> ${output}_records.json
  if [[ ! $i -eq $periods ]]; then 
    echo "," >> ${output}_records.json
  fi


  # increment
  start_utc=$end_utc

done

for m in $metrics; do
    echo "] } " >> ${output}_${m}.json
done
echo "] } " >> ${output}_records.json

$SCRIPT_DIR/parse-metrics.py ${output}
