#!/usr/bin/env python3

stats_file = open(sys.argv[1], 'r')
ratesFile = open(sys.argv[2], 'x')
heavyFile = open(sys.argv[3], 'x')

ratesFile.write("time,relation,rate\n")
heavyFile.write("time,relation,attribute,value\n")

for line in stats_file:
    if line[-1] == '\n':
        line = line[:-1]
    parts = line.split(';')
    ratesFile.write(parts[0] + '\n')
    time, relation = parts[0].split(',')[0:2]
    if relation == "":
        continue
    for i in range(1, len(parts)):
        if not parts[i]:
            continue
        for hitter in parts[i].split(','):
            heavyFile.write(time + ',' + relation + ',' + str(i - 1) + ',' + hitter + '\n')

