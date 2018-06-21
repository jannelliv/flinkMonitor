#!/usr/bin/env python3

import datetime
import re
import sys

main_start = datetime.datetime(2010, 8, 27, 15, 0, 0, tzinfo=datetime.timezone.utc)
main_end   = datetime.datetime(2010, 8, 28, 15, 0, 0, tzinfo=datetime.timezone.utc)
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
        if ts >= end_ts:
            break
        if ts < start_ts:
            past_output.write(line)
        else:
            main_output.write(line)

input_file.close()
past_output.close()
main_output.close()
