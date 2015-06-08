#!/usr/bin/env python
import itertools
import random
import argparse
import re
import subprocess
import os
import sys
import shlex
import time
import glob
from time import localtime, strftime
import config
import simmain

def shcmd(cmd, ignore_error=False):
    print 'Doing:', cmd
    ret = subprocess.call(cmd, shell=True)
    print 'Returned', ret, cmd
    if ignore_error == False and ret != 0:
        exit(ret)
    return ret

def run_and_get_output(cmd):
    output = []
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    p.wait()

    return p.stdout.readlines()

class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

def ParameterCombinations(parameter_dict):
    """
    Get all the cominbation of the values from each key
    http://tinyurl.com/nnglcs9
    Input: parameter_dict={
                    p0:[x, y, z, ..],
                    p1:[a, b, c, ..],
                    ...}
    Output: [
             {p0:x, p1:a, ..},
             {..},
             ...
            ]
    """
    d = parameter_dict
    return [dict(zip(d, v)) for v in itertools.product(*d.values())]

#########################################################
# Git helper
# you can use to get hash of the code, which you can put
# to your results
def git_latest_hash():
    cmd = ['git', 'log', '--pretty=format:"%h"', '-n', '1']
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE)
    proc.wait()
    hash = proc.communicate()[0]
    hash = hash.strip('"')
    print hash
    return hash

def git_commit(msg='auto commit'):
    shcmd('git commit -am "{msg}"'.format(msg=msg),
            ignore_error=True)

########################################################
# table = [
#           {'col1':data, 'col2':data, ..},
#           {'col1':data, 'col2':data, ..},
#           ...
#         ]
def table_to_file(table, filepath, adddic=None):
    'save table to a file with additional columns'
    with open(filepath, 'w') as f:
        colnames = table[0].keys()
        if adddic != None:
            colnames += adddic.keys()
        colnamestr = ';'.join(colnames) + '\n'
        f.write(colnamestr)
        for row in table:
            if adddic != None:
                rowcopy = dict(row.items() + adddic.items())
            else:
                rowcopy = row
            rowstr = [rowcopy[k] for k in colnames]
            rowstr = [str(x) for x in rowstr]
            rowstr = ';'.join(rowstr) + '\n'
            f.write(rowstr)

def generate_lba_workload_seq(flash_page_size,
                         flash_npage_per_block,
                         flash_num_blocks,
                         tofile
                         ):
    f = open(tofile, 'w')
    totalpages = flash_npage_per_block * flash_num_blocks
    for page in range(1, totalpages * 2):
        page = int(page % (totalpages * 0.3))
        offset = page * flash_page_size
        size = flash_page_size
        event = 'write {} {}'.format(offset, size)
        f.write(event + '\n')

    f.close()

def get_github_url():
    shcmd('../get_github_url.py analysis/analyzer.r')

def generate_lba_workload_random(flash_page_size,
                         flash_npage_per_block,
                         flash_num_blocks,
                         tofile
                         ):
    f = open(tofile, 'w')
    totalpages = flash_npage_per_block * flash_num_blocks
    random.seed(1)
    for page in range(0, totalpages * 3):
        page = int(random.random() * totalpages * 0.5)
        offset = page * flash_page_size
        size = flash_page_size
        event = 'write {} {}'.format(offset, size)
        f.write(event + '\n')

    f.close()

def generate_lba_workload(pattern):
    workloadfile = 'misc/tmpworkload'
    if pattern == 'seq':
        generate_lba_workload_seq(config.flash_page_size,
                                  config.flash_npage_per_block,
                                  config.flash_num_blocks,
                                  workloadfile
                                  )
    elif pattern == 'random':
        generate_lba_workload_random(config.flash_page_size,
                                  config.flash_npage_per_block,
                                  config.flash_num_blocks,
                                  workloadfile
                                  )

    # simmain.sim_run(workloadfile)

def run_r_script(scriptpath):
    shcmd('Rscript {}'.format(scriptpath))

def main():
    generate_lba_workload('seq')
    # generate_lba_workload('random')
    print 'workload generated'
    shcmd('./simmain.py -e misc/tmpworkload > tmp2')
    shcmd('sync')
    shcmd('grep RECORD tmp2 > ../analysis/data/sim.result.sample')
    shcmd('sync')
    run_r_script('../analysis/analyzer.r')

    # shcmd('./simmain.py -e misc/lbaevents.sample')

def _main():
    parser = argparse.ArgumentParser(
            description="This file hold command stream." \
            'Example: python Makefile.py doexp1 '
            )
    parser.add_argument('-t', '--target', action='store')
    args = parser.parse_args()

    if args.target == None:
        main()
    else:
        # WARNING! Using argument will make it less reproducible
        # because you have to remember what argument you used!
        targets = args.target.split(';')
        for target in targets:
            eval(target)

if __name__ == '__main__':
    _main()

