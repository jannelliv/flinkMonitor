import csv
import sys
import json
from pprint import pprint

"""
A simple program to print the result of a Prometheus query as CSV.
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

# Get the files
for i in range(len(data_max['all'])):
    for j in range(len(data_max['all'][i]['data']['result'])):
        file = data_max['all'][i]['data']['result'][j]['metric']['job_name']
        output_file = open("metrics_"+file, 'w')
        writer = csv.writer(output_file)
        writer.writerow(['timestamp', 'peak', 'max', 'average'])
        idx = len(data_max['all'][i]['data']['result'][j]['values'])
        for t in range(idx):
            ts = data_max['all'][i]['data']['result'][j]['values'][t][0]
            peak = data_peak['all'][i]['data']['result'][j]['values'][t][1]
            max = data_max['all'][i]['data']['result'][j]['values'][t][1]
            avg = data_avg['all'][i]['data']['result'][j]['values'][t][1]
            writer.writerow([ts, peak, max, avg])
        output_file.close()


f_max.close()
f_avg.close()
f_peak.close()
