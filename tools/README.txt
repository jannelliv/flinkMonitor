Replayer
========

Usage: java -jar target/evaluation-tools-1.0-SNAPSHOT.jar
       [-v|-vv] [-a <acceleration>] [-q <buffer size>] [-m] [-t <interval>] [-o <host>:<port>] <file>

<file>              Input file with event log in CSV format (CRV '14)

-v                  Write a compact report every second to stderr, format:
                    <elapsed time [s]>   <current index rate [1/s]> <current event rate [1/s]>   <current delay [s]> <max. delay in last second [s]> <max. delay [s]> <average delay [s]>

-vv                 Write a verbose report every second to stderr

-a <acceleration>   Speed up factor (default: 1). For example, a value of 2 will replay the log twice as fast.
                    Set this to 0 to replay the whole log as fast as possible.

-q <buffer size>    Size of the internal buffer (default: 64).
                    Increase this value if -vv repeatedly reports underruns.

-m                  Output in Monpoly format.

-t <interval>       Inject the current time into the output at regular intervals.
                    The interval is specified in milliseconds. The format of the timestamp lines is
                    "###<ts>", where <ts> is a Unix timestamps with millisecond resolution.

-o <host>:<port>    Open a TCP server listening on the given host name and port.
                    Only one client is accepted, to which the output is written.
                    If this option is not given, write to stdout.
