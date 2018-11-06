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
        offset = 0

        try:
            sample_map = monitor_map[m]
            sample_num=len(data_record['all'][i]['data']['result'][j]['values'])
            #print("sample map already exists for monitor: %d in job: %s" % (m, job))

            #for key, value in sample_map.items():
            #    print("Ts: %d, value: %d" % (key, value))
        except:
            sample_map = {}
            sample_num=len(data_record['all'][i]['data']['result'][j]['values'])

        for t in range(0,sample_num):
            ts = int(data_record['all'][i]['data']['result'][j]['values'][t][0])
            value =   int(data_record['all'][i]['data']['result'][j]['values'][t][1])
            sample_map[ts] = value - offset
            offset = value

        monitor_map[m]=sample_map
        job_map_record[job]=monitor_map





def extract(data):
    job_map_latency = {}
    for i in range(len(data['all'])):
        for j in range(len(data['all'][i]['data']['result'])):
            job = str(data['all'][i]['data']['result'][j]['metric']['job_name'])
            try:
                sample_map = job_map_latency[job]
                #print("Job %s already has stats" % job)

                #for key, value in sample_map.items():
                    #print("Ts: %d, value: %d" % (key, value))

                sample_num = len(data['all'][i]['data']['result'][j]['values'])
                for t in range(sample_num):
                    ts             = int(data['all'][i]['data']['result'][j]['values'][t][0])
                    value          = int(data['all'][i]['data']['result'][j]['values'][t][1])
                    #print("TS: %d, Value: %d" % (ts, value))
                    sample_map[ts] = value
            except:
                sample_map = {}
                sample_num = len(data['all'][i]['data']['result'][j]['values'])
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


def recalculateMax(latency):
    max_l = 0
    length = len(latency)
    for i in range(0, length):
        ts, tmp  = latency[i]
        max_l = max([max_l, tmp])
        latency[i] = (ts, max_l)

    return latency


def recalculateAverage(peak, average):
    avg_l = 0
    sum = 0
    length = len(average)
    offset = 0
    for i in range(1, length+1):
        ts, tmp  = peak[i-1]
        sum += tmp
        if sum == 0:
            offset += 1
        if tmp > 0:
            avg_l = sum / (i - offset)

        average[i-1] = (ts, avg_l)

    return average


def d2l(d):
    dictlist = []
    for key, value in d.iteritems():
        temp = [key,value]
        dictlist.append(temp)
    dictlist.sort(key=lambda x: x[0])
    return dictlist

def matchupLists(l1, l2):
    length = min([len(l1), len(l2)])

    for i in range(0,length):
        ts_1, a = l1[i]
        ts_2, b = l2[i]

        if ts_1 < ts_2:
            l2[i] = ts_2 - 1, b
        if ts_2 < ts_1:
            l1[i] = ts_1 -1, a

    return l1, l2




for job in common_jobs:
    output_file = open("metrics_"+job+".csv", 'w')
    writer = csv.writer(output_file)
    ms = len(job_map_record[job])
    writer.writerow(['timestamp', 'peak', 'max', 'average', 'sum_tp']+ map(lambda x: "monitor_tp"+str(x), range(0,ms)))
    
    index = 0
    #Gets lists from dicts
    peak_list = d2l(job_map_peak[job])
    max_list  = recalculateMax(d2l(job_map_max[job]))
    avg_list  = recalculateAverage(peak_list, d2l(job_map_avg[job]))

    job_record_dict = {}

    for m in range(ms):
        l = d2l(job_map_record[job][m])
        l, peak_list = matchupLists(l, peak_list)
        l, max_list = matchupLists(l, max_list)
        l, avg_list = matchupLists(l, avg_list)
        job_record_dict[m]= l


    #try:
    #    index = job.index("ft")
    length = len(job_map_max[job])
    offset = {}

    for i in range(0,length):
        skip=False
        ts_p, peak = peak_list[i]
        ts_m, max_n  = max_list[i]
        ts_a, avg  = avg_list[i]

        #assert (ts_a == ts_m == ts_p), "latency timestamps are misaligned"
        ts = min(ts_p,ts_m,ts_a)
        r = [ts,peak,max_n,avg]
        records = []
        for m in range(ms):
            try:
                ts_r, rec = job_record_dict[m][i]
                if not m in offset:
                    offset[m] = 0
                ts_r += offset[m]
                records = records + [rec]
            except IndexError:
                records = records + [0]
                # number of samples misaligned
                #print("Number of samples misaligned")
                #print("Len: %d vs Len: %d" % (len(job_map_max[job]), len(d2l(job_map_record[job][m]))))
                #print("Job: %s, m: %d, i: %d" % (job, m, i))
        if skip:
            continue
        sum_tp = sum(records)
        r = r + [sum_tp] + records
        writer.writerow(r)
    #except:
    #    ()
    output_file.close()

f_max.close()
f_avg.close()
f_peak.close()
f_record.close()
