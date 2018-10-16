Parallel Online Monitor implemented via distributed stream processors

      Joshua Schneider, Srđan Krstić and Dmitriy Traytel
    Department of Computer Science, ETH Zurich, Switzerland


This library is distributed under the terms of the GNU Lesser General
Public License version 3. See files LICENSE and COPYING.


Arguments for ch.eth.inf.infsec.StreamMonitoring
------------------------------------------------

Required arguments: --out, --sig, --formula

--checkpoints <URI>         If set, checkpoints are saved in the directory at <URI>
                            Example: --checkpoints file:///foo/bar

--in <file>                 Read events from <file> (see also: --watch, default: 127.0.0.1:9000)
--in <host>:<port>          Connect to the given TCP server and read events

--format monpoly|csv        Format of the input events (default: monpoly)

--watch true|false          If set to true, the argument of --in is interpreted as a directory (default: false).
                            The monitor watches for new files in the directory and reads events from them.

--out <file>                Write verdicts to <file>, which must not exist
--out <host>:<port>         Connect to the given TCP server and write verdicts

--monitor <command>         Name of the monitor executable and additional arguments (default: "monpoly -negate")

--sig <file>                Name of the signature file

--formula <file>            Name of the file with the MFOTL formula

--processors <N>            Number of parallel monitors, must be a power of two (default: 1)

--shares <var>=<N>,...      If set, use the specified shares for partitioning
                            Example: --processors 8 --shares x=2,y=1,z=4

--rates <relation>=<N>,...  If set, use the specified relation sizes in the hypercube optimization
                            Example: --rates publish=100,approve=50

--heavy <file>              If set, read a list of heavy hitters from <file>.
                            Each line has the format "<relation>,<attribute>,<value>", where <attribute>
                            is a zero-based index.


Arguments for ch.eth.inf.infsec.analysis.OfflineAnalysis
--------------------------------------------------------

Required arguments: --log, --out

--log <file>                Read events from <file>

--out <file>                Write statistics to <file>, see below for output format

--format monpoly|csv        Format of the input events (default: monpoly)

--window <size>             Window size in seconds for which the statistics are computed (default: 60)

--degree <N>                Number of parallel monitors, used to determine heavy hitters (default: 16)

--formula <file>            If set, read the MFOTL formula from <file> and use it for filtering/optimization

--shares <var>=<N>,...
--rates <relation>=<N>,...
--heavy <file>              See above. If any of these is set, compute statistics for individual slices.

--collect-heavy true|false  Whether heavy hitters should be collected (default: true)
--with-counts true|false    Whether frequencies of all heavy hitters should be collected (default: false)
--threshold <N>             Minimum threshold for heavy hitter classification (default: 0)


Output format for slice statistics (if --rates or --shares is set):
<start time of window>,<slice id>,<relation>,<count>

Output format for global statistics (--collect-heavy true):
<start time of window>,<relation>,<count>;<heavy hitter for attribute #0>,...;<heavy hitter for attribute #1>,...;...

Output format for global statistics with heavy hitters frequencies (--collect-heavy true --with-counts true):
<start time of window>,<relation>,<count>;<heavy hitter for attribute #0>,<count>,<heavy hitter>,<count>,...;...

The count for the empty relation name refers to the number of time-points.


Utilities
---------

Replayer
    See tools/README.md

tools/split_statistics.py <rates> <heavy>
    Reads OfflineAnalysis statistics from standard input and splits it into two files with event rates and heavy hitters, respectively.
    The format of the heavy hitter file is: <start time of window>,<relation>,<attribute index (0-based)>,<value>

tools/merge_heavy.py
    Reads the heavy hitter file as produced by split_statistics.py from standard input and merges all windows. This removes the first column.

evaluation/nokia/cut_log.py <input> <past output> <main output>
    Reads the "Nokia" CSV log file from <input> and writes two hard-coded time intervals to the given files.
