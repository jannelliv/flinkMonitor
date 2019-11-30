#!/usr/bin/env python3

import argparse
import os
import sys
import subprocess
import tempfile
import shutil
import glob
import atexit
import time
from subprocess import CompletedProcess
from verdicts_diff import verify_verdicts
from typing import List


def del_tmp():
    pass
    shutil.rmtree(tmp_dir)


def fail(s: str):
    print(s)
    print("=== TEST FAILED ===")
    sys.exit(1)


def exe_cmd(cmd: List[str], print_stdout: bool) -> CompletedProcess:
    p = subprocess.run(cmd, capture_output=True)
    if p.returncode != 0:
        fail('Error: failed to execute cmd ' + (" ".join(p.args)) + "\n\n" + p.stderr.decode(sys.getdefaultencoding()))
    stderr = p.stderr.decode(sys.getdefaultencoding())
    if stderr.strip():
        print(p.stderr)
    if print_stdout:
        print(p.stdout.decode(sys.getdefaultencoding()))
    return p


def launch_parallel(cmds: List[List[str]]):
    procs = [subprocess.Popen(cmd, stderr=subprocess.PIPE) for cmd in cmds]
    if len(procs) == 0:
        return
    while True:
        for proc in procs:
            retcode = proc.poll()
            if retcode is not None:
                procs.remove(proc)
                if retcode != 0:
                    for p in procs:
                        p.kill()
                    _, stderr = proc.communicate()
                    fail('Error: failed to execute cmd ' + (" ".join(proc.args)) + "\n\n"
                            + stderr.decode(sys.getdefaultencoding()))
                if len(procs) == 0:
                    return
            else:
                time.sleep(0.05)


def pipe(cmd : List[str], o_path: str, print_stdout : bool = False):
    p = exe_cmd(cmd, print_stdout)
    f = open(o_path, 'wb')
    f.write(p.stdout)
    f.close()


parser = argparse.ArgumentParser(description='Run simple synthetic tests')
parser.add_argument('-e', '--event_rate', default=1000, type=int)
parser.add_argument('-i', '--index_rate', default=10, type=int)
parser.add_argument('-f', '--formula', default='triangle-neg.mfotl')
parser.add_argument('-s', '--signature', default='synth.sig')
parser.add_argument('-p', '--processors', default=2, type=int)
parser.add_argument('-n', '--kafkaparts', default=4, type=int)
parser.add_argument('-m', '--multisource_variant', default=1, type=int)
parser.add_argument('-r', '--use_replayer', default=False, type=bool)
parser.add_argument('-a', '--replayer_accel', default=1.0, type=float)
parser.add_argument('flink_dir')
args = parser.parse_args()

tmp_dir = tempfile.mkdtemp()
atexit.register(del_tmp)
print("Temp dir is: " + tmp_dir)
work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
jar_path = work_dir + '/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar'
data_dir = work_dir + '/evaluation'
sig_file = data_dir + '/synthetic/' + args.signature
formula_file = data_dir + '/synthetic/' + args.formula
collapse = False
use_replayer = args.use_replayer

if args.multisource_variant == 1:
    collapse = False
elif args.multisource_variant == 2 or args.multisource_variant == 3 or args.multisource_variant == 4:
    collapse = True
else:
    fail("Invalid value for arg multisource_variant ")

if use_replayer and args.multisource_variant == 3:
    fail("multisouce variant 3 (watermark order without explicit emissiontime) is not compatible with the replayer")

if (not use_replayer) and args.multisource_variant == 4:
    fail("multisource variant 4 (watermark order with explicit emissiontime) is only compatible with the replayer")

if not os.path.isfile(jar_path):
    fail("Error: Could not find monitor jar " + jar_path)

if (not os.path.isdir(args.flink_dir)) or (not os.path.isfile(args.flink_dir + '/bin/flink')):
    fail("Error: Flink dir is invalid")

print('Generating log ...')
pipe([work_dir + '/generator.sh', '-T', '-e', str(args.event_rate), '-i',
      str(args.index_rate), '-x', '1', '30'], tmp_dir + '/trace.csv')

print('Preprocessing log for multiple inputs ...')

exe_cmd([work_dir + '/trace-transformer.sh', '-v', str(args.multisource_variant), '-n', str(args.kafkaparts),
         '-o', tmp_dir + '/preprocess_out', tmp_dir + '/trace.csv'], False)

print('Converting to monpoly format for validation ...')
pipe([work_dir + '/replayer.sh', '-i', 'csv', '-f', 'monpoly', '-a', '0',
      tmp_dir + '/trace.csv'], tmp_dir + '/trace.log')

print("Creating reference output ...")
pipe(['monpoly', '-sig', sig_file, '-formula', formula_file, '-log',
      tmp_dir + '/trace.log'], tmp_dir + '/reference.txt')

flink_args = [args.flink_dir + '/bin/flink', 'run', jar_path, '--in', 'kafka',
         '--format','csv', '--sig', sig_file, '--formula', formula_file, '--negate', 'false', '--multi',
         str(args.multisource_variant), '--monitor', 'monpoly', '--processors', str(args.processors), '--out',
         tmp_dir + '/flink-out', '--clear', str(not use_replayer), '--nparts', str(args.kafkaparts)]

if use_replayer:
    print('Launching replayer and running monitor ...')
    replayer_args = [work_dir + '/replayer.sh', '-i', 'csv', '-f', 'csv',
                     '-n', str(args.kafkaparts), '-a', str(args.replayer_accel), '--clear']
    if args.multisource_variant == 1:
        replayer_args += ['--term', 'TIMEPOINTS']
    elif args.multisource_variant == 2:
        replayer_args += ['--term', 'TIMESTAMPS']
    elif args.multisource_variant == 4:
        replayer_args += ['--term', 'NO_TERM', '-e']
    replayer_args += [tmp_dir + '/preprocess_out']
    launch_parallel([replayer_args, flink_args])
else:
    print("Running Flink monitor ...")
    flink_args += ['--kafkatestfile', tmp_dir + '/preprocess_out']
    exe_cmd(flink_args, True)


flink_out_file = [f for f in glob.glob(tmp_dir + '/flink-out/**', recursive=True) if os.path.isfile(f)]

if len(flink_out_file) < 1:
    fail("Error: Flink output file not found")

flink_out_file = flink_out_file[0]
shutil.copyfile(flink_out_file, tmp_dir + '/out.txt')

if verify_verdicts(collapse, tmp_dir + '/reference.txt', tmp_dir + '/out.txt'):
    print("=== TEST PASSED ===")
else:
    fail("")
