#!/usr/bin/env python3

import re
import sys

if len(sys.argv) != 3:
    print("Usage: verdicts_diff.py <reference> <verdicts>")
    sys.exit(2)

verdict_re = re.compile(r'@(\d+). \(time point (\d+)\): (.+)$')
tuple_re = re.compile(r'\(((?:[^",)]*|"[^"]*")(?:,[^",)]*|"[^"]*")*)\)')
def read_verdicts(path, fail_unknown):
    unknown = 0
    verdicts = []
    with open(path, 'r') as f:
        for line in f:
            line = line.rstrip()
            match = verdict_re.match(line)
            if match is None:
                if fail_unknown:
                    print("Error: Unknown line in reference file: " + line)
                    sys.exit(2)
                else:
                    print("UNKNOWN " + line)
                    unknown += 1
            else:
                ts = match.group(1)
                tp = match.group(2)
                data = match.group(3)
                tuples = []
                if data == 'true':
                    tuples.append(())
                else:
                    for tuple_match in tuple_re.finditer(data):
                        # TODO: parse quoted parameters correctly
                        params = map(lambda x: x.strip(), tuple_match.group(1).split(','))
                        tuples.append(tuple(params))
                for tup in tuples:
                     verdicts.append((tp,ts) + tup)

    verdicts.sort()
    return verdicts, unknown

reference, _ = read_verdicts(sys.argv[1], True)
verdicts, unknown = read_verdicts(sys.argv[2], False)

def verdict_str(v):
    tp = v[0]
    ts = v[1]
    params = v[2:]
    return '@' + ts + ' (' + tp +'): ' + ', '.join(params)

diff = 0
r = 0
v = 0
while r < len(reference) or v < len(verdicts):
    have_r = r < len(reference)
    have_v = v < len(verdicts)
    if have_v and (not have_r or verdicts[v] < reference[r]):
        print("ADDED   " + verdict_str(verdicts[v]))
        diff += 1
        v += 1
    elif have_r and (not have_v or verdicts[v] > reference[r]):
        print("MISSING " + verdict_str(reference[r]))
        diff += 1
        r += 1
    else:
        v += 1
        r += 1

if unknown > 0 or diff > 0:
    sys.exit(1)
