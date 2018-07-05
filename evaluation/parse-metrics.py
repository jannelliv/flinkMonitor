#!/usr/bin/env python

import csv
import sys
import json
from pprint import pprint

"""
A simple program to print the result of a Prometheus range query as CSV.
"""

if len(sys.argv) != 2:
    print("Error: Invalid arguments. Please specify path to the"+
    " common prefix of the three json files. \n For example" + 
    " ./parse-metrics.py ~/nokia")
    sys.exit(1)


with open(sys.argv[1]+"_max.json") as f_max:
    data_max = json.load(f_max)

with open(sys.argv[1]+"_average.json") as f_avg:
    data_avg = json.load(f_avg)

with open(sys.argv[1]+"_peak.json") as f_peak:
    data_peak = json.load(f_peak)

with open(sys.argv[1]+"_records.json") as f_record:
    data_record = json.load(f_record)

job_map_record = {}

for i in range(len(data_record['all'])):
    for j in range(len(data_record['all'][i]['data']['result'])):
        job = str(data_record['all'][i]['data']['result'][j]['metric']['job_name'])
        try:
            monitor_map=job_map_record[job]
        except:
            monitor_map={}
        m = int(data_record['all'][i]['data']['result'][j]['metric']['subtask_index'])
        sample_map = {}
        sample_num=len(data_record['all'][i]['data']['result'][j]['values'])
        for t in range(0,sample_num):
            ts = int(data_record['all'][i]['data']['result'][j]['values'][t][0])
            sample_map[ts] = int(data_record['all'][i]['data']['result'][j]['values'][t][1])
        monitor_map[m]=sample_map
        job_map_record[job]=monitor_map


def extract(data):
    job_map_latency = {}
    for i in range(len(data['all'])):
        for j in range(len(data['all'][i]['data']['result'])):
            job = str(data['all'][i]['data']['result'][j]['metric']['job_name'])
            sample_num = len(data['all'][i]['data']['result'][j]['values'])
            sample_map = {}
            for t in range(sample_num):
                ts             = int(data['all'][i]['data']['result'][j]['values'][t][0])
                sample_map[ts] = int(data['all'][i]['data']['result'][j]['values'][t][1])
            job_map_latency[job] = sample_map
    return job_map_latency

job_map_max  = extract(data_max)
job_map_avg  = extract(data_avg)
job_map_peak = extract(data_peak)

print(str(len(job_map_record)) + " record jobs extracted")
print(str(len(job_map_max)) + " max jobs extracted")
print(str(len(job_map_avg)) + " avg jobs extracted")
print(str(len(job_map_peak)) + " peak jobs extracted")

common_jobs = set(job_map_record.keys()).intersection(set(job_map_max.keys()))
common_jobs = common_jobs.intersection(job_map_avg.keys())
common_jobs = common_jobs.intersection(job_map_peak.keys())

#additional jobs
non_common_jobs = set(job_map_record.keys())-common_jobs
non_common_jobs = non_common_jobs.union(set(job_map_max.keys())-common_jobs)
non_common_jobs = non_common_jobs.union(set(job_map_avg.keys())-common_jobs)
non_common_jobs = non_common_jobs.union(set(job_map_peak.keys())-common_jobs)
print("Skipping the "+ str(len(non_common_jobs)) +" jobs not in common: " + str(non_common_jobs))


def d2l(d):
    dictlist = []
    for key, value in d.iteritems():
        temp = [key,value]
        dictlist.append(temp)
    return dictlist

for job in common_jobs:
    output_file = open("metrics_"+job+".csv", 'w')
    writer = csv.writer(output_file)
    ms = len(job_map_record[job])
    writer.writerow(['timestamp', 'peak', 'max', 'average']+ map(lambda x: "monitor"+str(x), range(0,ms)))
    
    index = 0
    peak_list= d2l(job_map_peak[job])
    max_list = d2l(job_map_max[job])
    avg_list = d2l(job_map_avg[job])
    for i in range(0,len(job_map_max[job])):
        skip=False
        ts_p, peak = peak_list[i]
        ts_m, max  = max_list[i]
        ts_a, avg  = avg_list[i]
        assert (ts_a == ts_m == ts_p), "latency timestamps are misaligned"
        ts = min(ts_p,ts_m,ts_a)
        r = [ts,peak,max,avg]
        for m in range(ms):
            try:
                ts_r, rec = d2l(job_map_record[job][m])[i]
                if ts < ts_r:
                    skip=True
                    break
                if ts > ts_r:
                    continue
                r = r + [rec] 
            except IndexError:
                # number of samples misaligned
                break
        if skip:
            continue

        writer.writerow(r)
    output_file.close()

f_max.close()
f_avg.close()
f_peak.close()
f_record.close()
