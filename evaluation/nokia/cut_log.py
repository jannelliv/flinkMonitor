#!/usr/bin/env python3

import datetime
import re
import sys

main_start = datetime.datetime(2010, 8, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
main_end   = datetime.datetime(2010, 8, 30, 12, 0, 0, tzinfo=datetime.timezone.utc)
past_reach = datetime.timedelta(hours=30)

start_ts = int((main_start - past_reach).timestamp())
end_ts = int(main_end.timestamp())

ts_matcher = re.compile(r"ts\s*=\s*(\d+)\s*,")
def get_ts(line):
    return int(ts_matcher.search(line).group(1))

for line in sys.stdin:
    ts = get_ts(line)
    if ts >= start_ts:
        if ts >= end_ts:
            break
        sys.stdout.write(line)
