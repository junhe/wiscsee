import subprocess
import os

from utilities import utils

class LevelDBProc(object):
    def __init__(self, benchmarks, num, db, outputpath,
            threads, use_existing_db, max_key, max_log):
        self.benchmarks = benchmarks
        self.num = num
        self.db = db
        self.outputpath = outputpath
        self.threads = threads
        self.use_existing_db = use_existing_db
        self.max_key = max_key
        self.max_log = max_log

    def run(self):
        utils.prepare_dir(self.db)

        db_bench_path = "../leveldb/db_bench"
        cmd = "{exe} --benchmarks={benchmarks} --num={num} --db={db} "\
                "--threads={threads}  "\
                "--dowrite_max_key={max_key} "\
                "--dowrite_skew_max_log={max_log} "\
                "--use_existing_db={use_existing_db} > {out}"\
            .format(
                exe = db_bench_path,
                benchmarks = self.benchmarks,
                num = self.num,
                db = self.db,
                out = self.outputpath,
                threads = self.threads,
                max_key = self.max_key,
                max_log = self.max_log,
                use_existing_db = self.use_existing_db
                )
        print cmd
        self.p = subprocess.Popen(cmd, shell=True)
        return self.p

    def wait(self):
        self.p.wait()



class SqliteProc(object):
    def __init__(self, n_insertions, pattern, db_dir):
        self.n_insertions = n_insertions
        self.pattern = pattern
        self.db_dir = db_dir
        self.db_path = os.path.join(self.db_dir, 'data.db')
        self.p = None

    def run(self):
        bench_path = './sqlitebench/bench.py'

        utils.prepare_dir_for_path(self.db_path)

        cmd = 'python {exe} -f {f} -n {n} -p {p}'.format(
            exe=bench_path, f=self.db_path, n=self.n_insertions, p=self.pattern)

        print cmd
        self.p = subprocess.Popen(cmd, shell=True)

        return self.p

    def wait(self):
        self.p.wait()





PART1 = """
set $dir={dirpath}
set $nfiles=8000
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

class VarmailProc(object):
    def __init__(self, dirpath, seconds):
        self.dirpath = dirpath
        self.seconds = seconds
        self.hash_str = str(hash(dirpath))
        self.conf_path = '/tmp/filebench.config.' + self.hash_str
        self.p = None

    def run(self):
        conf_text = self.get_conf_text()
        with open(self.conf_path, 'w') as f:
            f.write(conf_text)

        utils.prepare_dir(self.dirpath)

        cmd = 'filebench -f {}'.format(self.conf_path)
        self.p = subprocess.Popen(cmd, shell=True)

        return self.p

    def get_conf_text(self):
        return PART1.format(dirpath=self.dirpath) + PART2 + PART3.format(self.seconds)

    def wait(self):
        self.p.wait()


