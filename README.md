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
