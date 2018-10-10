#!/usr/bin/env python3

import datetime
import re
import sys


def processLine(relation, arr):
    if relation == "select" or relation == "insert" or relation == "update" or relation == "delete":
        return '%s, %s, %s, %s, %s, %s, %s\n' % (relation, arr[1].strip(), arr[2].strip(), wrapString(arr[3]), wrapString(arr[4]), wrapString(arr[5]), wrapString(arr[6]))
    elif relation == "script_start" or relation == "script_end":
        return '%s, %s, %s, %s\n' % (relation, arr[1].strip(), arr[2].strip(), wrapString(arr[3]))
    elif relation == "script_svn":
        return '%s, %s, %s, %s, %s, %s, %s, %s\n' % (relation, arr[1].strip(), arr[2].strip(), wrapString(arr[3]), wrapString(arr[4]), wrapString(arr[5]), arr[6].strip(), arr[7].strip())
    elif relation == "script_md5":
        return '%s, %s, %s, %s, %s\n' % (relation, arr[1].strip(), arr[2].strip(), wrapString(arr[3]), wrapString(arr[4]))
    elif relation == "commit":
        return '%s, %s, %s, %s, %s\n' % (relation, arr[1].strip(), arr[2].strip(), wrapString(arr[3]), arr[4].strip())
    else:
        raise ValueError('Unsupported relation found: %s' % relation)


def wrapString(kv):
    (k, v) = kv.split("=")
    return '%s = "%s"' % (k.strip(), v.strip())



main_start = datetime.datetime(2010, 8, 27, 15, 0, 0, tzinfo=datetime.timezone.utc)
main_end   = datetime.datetime(2010, 8, 29, 17, 0, 0, tzinfo=datetime.timezone.utc)
past_reach = datetime.timedelta(hours=30)

past_ts = int((main_start - past_reach).timestamp())
start_ts = int(main_start.timestamp())
end_ts = int(main_end.timestamp())

if len(sys.argv) != 4:
    print("Error: Invalid arguments.")
    sys.exit(1)

input_file = open(sys.argv[1], 'r')
past_output = open(sys.argv[2], 'w')
main_output = open(sys.argv[3], 'w')

ts_matcher = re.compile(r"ts\s*=\s*(\d+)\s*,")
def get_ts(line):
    return int(ts_matcher.search(line).group(1))

for line in input_file:
    ts = get_ts(line)
    if ts >= past_ts:
        arr = line.split(",")
        pline = processLine(arr[0], arr)
        if ts >= end_ts:
            break
        if ts < start_ts:
            past_output.write(pline)
        else:
            main_output.write(pline)

input_file.close()
past_output.close()
main_output.close()


