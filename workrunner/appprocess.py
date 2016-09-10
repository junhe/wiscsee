import subprocess
import shlex
import os
import time
import random

from utilities import utils

import prepare4pyreuse
from pyreuse.sysutils.ftrace import *


class AppBase(object):
    def wait(self):
        self.p.wait()

    def terminate(self):
        self.p.terminate()
        del self.p


class LevelDBProc(AppBase):
    def __init__(self, benchmarks, num, db,
            threads, use_existing_db, max_key, max_log):
        self.benchmarks = benchmarks
        self.num = num
        self.db = db
        self.threads = threads
        self.use_existing_db = use_existing_db
        self.max_key = max_key
        self.max_log = max_log
        self.p = None

    def run(self):
        utils.prepare_dir(self.db)

        db_bench_path = "../leveldb/db_bench"
        cmd = "{exe} --benchmarks={benchmarks} --num={num} --db={db} "\
                "--threads={threads}  "\
                "--dowrite_max_key={max_key} "\
                "--dowrite_skew_max_log={max_log} "\
                "--use_existing_db={use_existing_db}"\
            .format(
                exe = db_bench_path,
                benchmarks = self.benchmarks,
                num = self.num,
                db = self.db,
                threads = self.threads,
                max_key = self.max_key,
                max_log = self.max_log,
                use_existing_db = self.use_existing_db
                )
        print cmd
        cmd = shlex.split(cmd)
        self.p = subprocess.Popen(cmd)
        return self.p


class SqliteProc(AppBase):
    def __init__(self, n_insertions, pattern, db_dir, commit_period, max_key):
        self.n_insertions = n_insertions
        self.pattern = pattern
        self.db_dir = db_dir
        self.db_path = os.path.join(self.db_dir, 'data.db')
        self.p = None
        self.commit_period = commit_period
        self.max_key = max_key

    def run(self):
        bench_path = './sqlitebench/bench.py'

        utils.prepare_dir_for_path(self.db_path)

        cmd = 'python {exe} -f {f} -n {n} -p {p} -e {e} -m {m}'.format(
            exe=bench_path, f=self.db_path, n=self.n_insertions, p=self.pattern,
            e=self.commit_period, m=self.max_key)

        print cmd
        cmd = shlex.split(cmd)
        self.p = subprocess.Popen(cmd)

        return self.p

    def wait(self):
        self.p.wait()

    def terminate(self):
        self.p.terminate()
        del self.p




PART1 = """
set $dir={dirpath}
set $nfiles={nfiles}
set $meandirwidth=1000000
set $filesize=cvar(type=cvar-gamma,parameters=mean:16384;gamma:1.5)
set $nthreads=16
set $iosize=1m
set $meanappendsize=16k
"""

PART2 = """
define fileset name=bigfileset,path=$dir,size=$filesize,entries=$nfiles,dirwidth=$meandirwidth,prealloc=80

define process name=filereader,instances=1
{
  thread name=filereaderthread,memsize=10m,instances=$nthreads
  {
    flowop deletefile name=deletefile1,filesetname=bigfileset
    flowop createfile name=createfile2,filesetname=bigfileset,fd=1
    flowop appendfilerand name=appendfilerand2,iosize=$meanappendsize,fd=1
    flowop fsync name=fsyncfile2,fd=1
    flowop closefile name=closefile2,fd=1
    flowop openfile name=openfile3,filesetname=bigfileset,fd=1
    flowop readwholefile name=readfile3,fd=1,iosize=$iosize
    flowop appendfilerand name=appendfilerand3,iosize=$meanappendsize,fd=1
    flowop fsync name=fsyncfile3,fd=1
    flowop closefile name=closefile3,fd=1
    flowop openfile name=openfile4,filesetname=bigfileset,fd=1
    flowop readwholefile name=readfile4,fd=1,iosize=$iosize
    flowop closefile name=closefile4,fd=1
  }
}

echo  "Varmail Version 3.0 personality successfully loaded"

"""

PART3  = "run {}"

class VarmailProc(AppBase):
    def __init__(self, dirpath, seconds, nfiles):
        self.dirpath = dirpath
        self.seconds = seconds
        self.nfiles = nfiles # 8000 was often used
        self.hash_str = str(hash(dirpath))
        self.conf_path = '/tmp/filebench.config.' + self.hash_str
        self.p = None

    def run(self):
        conf_text = self.get_conf_text()
        with open(self.conf_path, 'w') as f:
            f.write(conf_text)

        utils.prepare_dir(self.dirpath)

        cmd = 'filebench -f {}'.format(self.conf_path)
        print cmd
        self.p = subprocess.Popen(cmd, shell=True)

        return self.p

    def get_conf_text(self):
        return PART1.format(dirpath=self.dirpath, nfiles=self.nfiles) + \
                PART2 + PART3.format(self.seconds)

    def terminate(self):
        raise NotImplementedError('this sometimes not work')
        # utils.shcmd('pkill filebench')
        # del self.p


class F2FSTester(AppBase):
    def __init__(self, dirpath):
        self.dirpath = dirpath

    def run(self):
        utils.prepare_dir(self.dirpath)

        time.sleep(2)

        fileids = range(8000)

        for fileid in fileids:
            self.append_file(fileid, sync=False)

        live_set = set(fileids)
        dead_set = set()
        for i in range(1000):
            # delete one
            to_del = random.choice(list(live_set))
            self.delete_file(to_del)
            live_set.remove(to_del)
            dead_set.add(to_del)

            # append one
            to_append = random.choice(list(live_set))
            self.append_file(to_append)

    def stat_file(self, fileid):
        filename = 'f'+str(fileid)
        path = os.path.join(self.dirpath, filename)

        try:
            os.stat(path)
        except OSError:
            pass

    def delete_file(self, fileid):
        filename = 'f'+str(fileid)
        path = os.path.join(self.dirpath, filename)

        os.remove(path)

    def append_file(self, fileid, sync=True):
        filename = 'f'+str(fileid)
        path = os.path.join(self.dirpath, filename)

        nbytes = random.randint(4096, 4096*8)
        with open(path, 'a') as f:
            f.write('a'*nbytes)
            if sync:
                os.fsync(f)

    def wait(self):
        pass

    def terminate(self):
        pass






