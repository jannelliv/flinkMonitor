Trace Generator
===============

Usage:

./generator.sh {-S | -L | -T | -P <pattern>}
               [-e <event rate>] [-i <index rate>] [-t <timestamp>]
               [-x <violation ratio>] [-w <interval>]
               [-pA <event ratio A>] [-pB <event ratio B>]
               [-z <Zipf exponents>] <trace length>

Option summary:

    <trace length>
            The length of the trace in seconds.

    -S | -L | -T
            Selects the star, linear, or triangle event pattern (see below).

    -P <pattern>
            Specifies a custom event pattern. A pattern consists of three event
            names, each followed by a comma-separated list of variables
            enclosed in parentheses.

    -e <event rate>
            The total number of events per second of the trace.

    -i <index rate>
            The number of indexes (time-points) per second.

    -t <timestamp>
            The initial timestamp used for the first events.

    -x <violation ratio>
            The frequency of violations, relative to the total number of
            events.

    -w <interval>
            The interval in seconds between base and implied events, and
            between implied and forbidden events.

    -pA <event ratio A>
            The relative frequency of base events (type A).

    -pB <event ratio B>
            The relative frequency of implied events (type B).

    -z <Zipf exponents>
            Selects the Zipf distribution instead the uniform distribution for
            some variables. The argument is a comma-separated list of
            assignments <var>=<exp>[+<off>], where var is the name of variable,
            exp is the non-negative exponent of the Zipf distribution, and
            <off> is an optional offset. For example, if <off> is 100, the
            distribution has support {101, 102, ...}, with 101 being the most
            frequent value.


The trace generator prints a random trace of timestamped events to the standard
output. The trace has an indicated length of the given number of seconds.
However, the whole trace is produced at once and not as a real-time stream.
Simultaneous events are grouped into time-points, which are also called
indexes. The number of indexes per trace second is specified using option -i
(default: 1). The total number of events per second is given by option -e
(default: 10).

Events are parametrized by integer attributes. Two events are said to match if
the corresponding attributes are equal. There are three types of events:

  - Base events (type A).

  - Implied events (type B). For every base event, a matching implied event is
    added within the specified interval (option -w, default: 10).

  - Forbidden events (type C). A forbidden event that matches both a B event in
    the preceding interval and an A event before that (within another interval
    of the same size) constitutes a violation.

Events of the three types are generated randomly and independently according
to the event ratios -pA and -pB (default: one third each). The ratio of type
C is implied by the ratios of type A and B because the sum is always 1. The
frequency of violations relative to the number of events is set with option -x
(default: 0.01).

The attributes of the events and their correspondence must either be specified
as a custom pattern, or selected from a set of built-in patterns. A pattern
consists of names for the three event types, each followed by a non-empty list
of attributes represented by variables. Whenever a variable appears in multiple
attributes, then those attributes correspond to each other, and they must have
equal values in a match. A custom pattern is supplies as a single argument
after option -P. For example,

    ./generator.sh -P "e1(x) e2(x,y,z) e3(y,z)" 10

generates a 10 second trace, where the names of the events A, B, and C are e1,
e2, and e3, respectively. Exactly three event types must be specified. Events
e1 have one attribute, while events e2 have three. They match if they agree on
their first (only) attribute. Events e2 and e3 match if the second and third
attribute of e2 are equal to the first and second attribute of e3. Event and
variable names can be arbitrary alphanumeric strings, including hyphens and
underscores.

The built-in patterns are as follows:

    -S      Star pattern: A(w,x), B(w,y), C(w,z)
    -L      Linear pattern: A(w,x), B(x,y), C(y,z)
    -T      Triangle pattern: A(x,y), B(y,z), C(z,x)

By default, attribute values are chosen uniformly between 0 and 999 999 999. It
is also possible to use a Zipf distribution per variable. The exponents are
passed as an argument after option -z, together with optional offsets. For
example,

    ./generator.sh -T -z "x=1.5,z=2+100" 10

generates events according to the triangle pattern, where attributes with
variable x follow a Zipf distribution with exponent 1.5 starting at value 1.
Attributes with variable z follow a Zipf distribution with exponent 2 starting
at value 101 (100 + 1). Variable y has a uniform distribution.

Note that violations always use uniform values to prevent accidental matchings.
For the same reason, Zipf-distributed values of C events start at 1 000 001.

The output consists of lines with the following format, each representing
a single event:

    <event name>, tp=<index>, ts=<timestamp>, x0=<attribute #0>, x1=...

Indexes and timestamps start at zero. A different starting timestamp can be set
with option -t.


Replayer
========

Usage:

./replayer.sh [-v | -vv] [-a <acceleration>] [-q <buffer size>] [-m]
              [-t <interval>] [-o <host>:<port>] <file>

Option summary:

    <file>  Input file with the event log in modified CSV format.

    -v      Writes a compact report every second to stderr (see below).

    -vv     Writes a verbose report every second to stderr.

    -a <acceleration>
            Specifies the speed up factor (default: 1). For example, a value of
            2 will replay the log twice as fast. Set this to 0 to replay the
            whole log as quickly as possible.

    -q <buffer size>
            Sets the size of the internal buffer between the reader and write
            thread (default: 64). Increase this value if -vv repeatedly reports
            underruns.

    -m      Output in MonPoly format. If this option is not given, the output
            uses the modified CSV format.

    -t <interval>
            Injects the current time into the output at regular intervals. The
            interval is specified in milliseconds. The format of the timestamp
            lines is "###<ts>", where <ts> is a Unix timestamps with
            millisecond resolution. By default, no timestamps are injected.

    -o <host>:<port>
            Opens a TCP server listening on the given host name and port.
            Only a single client is accepted, to which the output is written.
            If this option is not given, writes to stdout.


The replayer reads events from a trace log in a modified CSV format. This is
the same format that was used in the First International Competition on
Software for Runtime Verification (RV 2014), and which is produced by the
trace generator. The replayer reproduces these events as a real-time stream
according to the events' timestamps. The events for the first timestamp found
in the log are written immediately to the output. The events for the next
timestamp are delayed proportionally to the difference of the timestamps, and
so on. The inverse of the proportionality factor is specified by option -a.

By default, the output is written in the same CSV format to standard output. It
is also possible to use the MonPoly format (option -m). If option -o together
with a hostname and port is given, a TCP server listening to that address is
created. The first client connecting to the TCP server will receive the event
stream. The first event is only sent once a client has connected. No more
clients will be accepted afterwards.

The replayer maintains some statistics about the number of events processed. It
also keeps track of the delay between the scheduled event time and the time the
event could be written to the output. The latter may be useful if another
process is reading the event stream via a pipe or socket. Use options -v or -vv
to retrieve a report once every second, which is written to standard error. The
reports contain both a peak and a maximum delay. The peak delay is the highest
delay that has been observed between the previous and the current measurement.
The maximum delay is the highest delay that has been observed so far. Note that
delays are tracked only up to the operating system's buffer that is associated
with the pipe or socket.

The implementation of the replayer uses two threads, one for reading and one
for writing events, which are connected by a queue with a limited capacity. If
the queue is drained fully, an underrun occurs and events may not be reproduced
at the appropriate time. The verbose report (-vv) displays the number of
underruns. If this number is non-zero and especially if it is growing, the
queue capacity should be increased with option -q.

The compact report consists of one line of measurements per second. Each line
consists of the following fields, which are separated by spaces:

  - elapsed time,
  - the current index rate (1/s),
  - the current event rate (1/s),
  - the current delay at the time of the measurement,
  - the peak delay observed between the previous and current measurements,
  - the maximum delay observed so far, and
  - the average delay.

All values are printed in seconds unless noted otherwise.
