#!/usr/bin/env python3

import argparse
import os
import sys
import subprocess
import tempfile
import shutil
import glob
from subprocess import CompletedProcess
from verdicts_diff import verify_verdicts
from typing import List


def fail(s: str):
    print(s)
    print("=== TEST FAILED ===")
    shutil.rmtree(tmp_dir)
    sys.exit(1)


def exe_cmd(cmd, print_stdout: bool) -> CompletedProcess:
    p = subprocess.run(cmd, capture_output=True)
    if p.returncode != 0:
        fail('Error: failed to execute cmd ' + (" ".join(p.args)) + "\n\n" + p.stderr.decode(sys.getdefaultencoding()))
    if print_stdout:
        print(p.stdout.decode(sys.getdefaultencoding()))
    return p


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
parser.add_argument('-m', '--multisource_variant', default=1, type=int)
parser.add_argument('flink_dir')
args = parser.parse_args()

tmp_dir = tempfile.mkdtemp()
print("Temp dir is: " + tmp_dir)
work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
jar_path = work_dir + '/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar'
data_dir = work_dir + '/evaluation'
sig_file = data_dir + '/synthetic/' + args.signature
formula_file = data_dir + '/synthetic/' + args.formula
collapse = False

if args.multisource_variant == 1:
    collapse = False
elif args.multisource_variant == 2 or args.multisource_variant == 3:
    collapse = True
else:
    fail("Invalid value for arg multisource_variant ")

if not os.path.isfile(jar_path):
    fail("Error: Could not find monitor jar " + jar_path)

if (not os.path.isdir(args.flink_dir)) or (not os.path.isfile(args.flink_dir + '/bin/flink')):
    fail("Error: Flink dir is invalid")

print('Generating log ...')
pipe([work_dir + '/generator.sh', '-T', '-e', str(args.event_rate), '-i',
      str(args.index_rate), '-x', '1', '60'], tmp_dir + '/trace.csv')

pipe([work_dir + '/replayer.sh', '-i', 'csv', '-f', 'monpoly', '-a', '0',
      tmp_dir + '/trace.csv'], tmp_dir + '/trace.log')

print("Creating reference output ...")
pipe(['monpoly', '-sig', sig_file, '-formula', formula_file, '-log',
      tmp_dir + '/trace.log'], tmp_dir + '/reference.txt')

print("Running Flink monitor ...")
exe_cmd([args.flink_dir + '/bin/flink', 'run', jar_path, '--in', 'kafka', '--kafkatestfile', tmp_dir + '/trace.csv',
         '--format','csv', '--sig', sig_file, '--formula', formula_file, '--negate', 'false', '--multi',
         str(args.multisource_variant), '--monitor', 'monpoly', '--processors', str(args.processors), '--out',
         tmp_dir + '/flink-out'], True)
flink_out_file = [f for f in glob.glob(tmp_dir + '/flink-out/**', recursive=True) if os.path.isfile(f)]

if len(flink_out_file) < 1:
    fail("Error: Flink output file not found")

flink_out_file = flink_out_file[0]
shutil.copyfile(flink_out_file, tmp_dir + '/out.txt')

if verify_verdicts(collapse, tmp_dir + '/reference.txt', tmp_dir + '/out.txt'):
    print("=== TEST PASSED ===")
else:
    fail("")
