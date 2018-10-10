#!/usr/bin/env python3

import sys

sys.stdin.readline()  # skip header
heavy = {}
for line in sys.stdin:
    row = line.split(',', 4)
    if len(row) != 4:
        continue
    (ts, relation, attrib, value) = row
    key = (relation, attrib, value)
    if key not in heavy:
        heavy[key] = 0
    heavy[key] += 1

for key, count in heavy.items():
    if count > 1:  # threshold
        sys.stdout.write(','.join(key))
