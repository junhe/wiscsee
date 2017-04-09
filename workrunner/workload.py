import collections
import os
import pprint
import random
import time
import sys
import subprocess

from commons import *
import config
import fio
import multiwriters
import perf
from utilities import utils
import workloadlist

from pyreuse.helpers import *
from pyreuse.sysutils.ftrace import trace_cmd
from pyreuse.apputils.parseleveldboutput import parse_file

class Workload(object):
    def __init__(self, confobj, workload_conf_key = None):
        """
        workload_conf is part of confobj. But we may need to run
        multiple workloads with different configurations in our
        experiements. So we need workload_conf to specify which
        configuration we will use in a Workload instance.

        Since workload_conf has a default value, it should be
        compatible with previous code. However, new classes based
        one Workload should use this new __init__() with two parameters.
        """
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__))

        self.conf = confobj
        if workload_conf_key != None and workload_conf_key != 'None':
            self.workload_conf = confobj[workload_conf_key]

    def run(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

class NoOp(Workload):
    """
    This is a workload class that does nothing. It may be used to skip
    the file system aging stage. To skip aging workload, set
    conf['age_workload_class'] = "NoOp"
    """
    def run(self):
        pass

    def stop(self):
        pass


class IterDirs(Workload):
    def run(self):
        mountpoint = self.conf['fs_mount_point']
        dirs = ['dir.'+str(i) for i in range(40)]
        files = ['file.'+str(i) for i in range(2)]

        for dirname in dirs:
            dirpath = os.path.join(mountpoint, dirname)
            print dirpath
            prepare_dir(dirpath)
            for filename in files:
                filepath = os.path.join(mountpoint, dirname, filename)
                print filepath
                self.write_file(filepath)

    def write_file(self, path):
        with open(path, 'w') as f:
            f.write('h' * int(2.1*MB))
            os.fsync(f)

class FileSnake(Workload):
    def run(self):
        conf = self.workload_conf['benchconfs']
        self.execute(conf['zone_len'], conf['snake_len'],
                conf['file_size'], conf['write_pre_file'])

    def execute(self, zone_len, snake_len, filesize, write_pre_file):
        print 'zone_len', zone_len, 'snake_len', snake_len
        dirpath = os.path.join(self.conf['fs_mount_point'], 'snake')
        utils.prepare_dir(dirpath)

        if write_pre_file is True:
            path = self.get_filepath('prefile')
            self.write_file(path, 124*KB)

        # tasks = self.generate_snake(zone_len, snake_len)
        tasks = self.generate_fading_snake(zone_len, snake_len)

        for i, op in tasks:
            path = self.get_filepath(i)
            if op == 'create':
                self.write_file(path, filesize)
            elif op == 'delete':
                self.rm_file(path)
            else:
                raise NotImplementedError()

    def get_filepath(self, i):
        filename = '{}.data'.format(i)
        path = os.path.join(self.conf['fs_mount_point'], 'snake', filename)
        return path

    def write_file(self, path, filesize):
        with open(path, 'w') as f:
            f.write('h' * int(filesize))
            os.fdatasync(f)

    def rm_file(self, path):
        os.remove(path)

    def generate_snake(self, zone_len, snake_len):
        q = collections.deque()
        tasks = []
        for i in range(zone_len):
            q.append(i)
            tasks.append( (i, 'create') )
            if len(q) == snake_len and i != zone_len - 1:
                j = q.popleft()
                tasks.append( (j, 'delete') )

        return tasks

    def generate_fading_snake(self, zone_len, snake_len):
        q = []
        tasks = []
        for i in range(zone_len):
            q.append(i)
            tasks.append( (i, 'create') )
            if len(q) == snake_len and i != zone_len - 1:
                j = random.choice(q)
                q.remove(j)
                tasks.append( (j, 'delete') )

        return tasks


class FIONEW(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(FIONEW, self).__init__(confobj, workload_conf_key)

        self.jobpath = os.path.join(self.conf['result_dir'],
            'fio_job_description.ini')

        self.resultpath = os.path.join(self.conf['result_dir'],
            'fio.report.txt')

        if not isinstance(self.workload_conf['ini'], fio.JobConfig):
            raise TypeError(
                "class {} is not of type class {}".
                format(type(self.workload_conf['ini']).__name__,
                    fio.JobConfig.__name__
                    ))

        self.to_json = self.workload_conf['runner'].get('to_json', True)

    def parse_results(self):
        d = utils.load_json(self.resultpath)
        table = fio.parse_results(d)
        utils.table_to_file(table, self.resultpath + '.parsed')

    def run(self):
        self.workload_conf['ini'].save(self.jobpath)

        if self.to_json == True:
            utils.prepare_dir_for_path(self.resultpath)
            fio_cmd = "fio {} --output-format=json --output {}".format(
                self.jobpath, self.resultpath)
        else:
            fio_cmd = "fio {}".format(self.jobpath)

        if self.conf['wrap_by_perf'] == True:
            perf.flamegraph_wrap(perf_path = self.conf['perf']['perf_path'],
                    cmd = fio_cmd,
                    result_dir = self.conf['result_dir'],
                    flamegraph_dir = self.conf['result_dir'])
                    # flamegraph_dir = self.conf['perf']['flamegraph_dir'])
        else:
            with utils.cd(self.conf['result_dir']):
                utils.shcmd(fio_cmd)

        if self.to_json:
            self.parse_results()

    def stop(self):
        pass


class SimpleRandReadWrite(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(SimpleRandReadWrite, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf["fs_mount_point"]
        datafile = os.path.join(mnt, "datafile")
        region = 2 * MB
        chunksize = 64 * KB
        n_chunks = region / chunksize
        chunkids = range(n_chunks)

        buf = "a" * chunksize
        f = open(datafile, "w+")
        random.shuffle(chunkids)
        for chunkid in chunkids:
            offset = chunkid * chunksize
            f.seek(offset)
            f.write(buf)
            os.fsync(f)

        random.shuffle(chunkids)
        for chunkid in chunkids:
            offset = chunkid * chunksize
            f.seek(offset)
            buf = f.read()
            os.fsync(f)

        f.close()

    def stop(self):
        pass


class WlMultiWriters(Workload):
    """
    """
    def translate(self, conf):
        """
        translate workload_conf to config used in multiwriters
        """
        nfiles = len(conf['filesizes'])
        fileids = range(nfiles)

        args_table = []
        for i in fileids:
            d = {   'file_size': conf['filesizes'][i],
                    'write_size': conf['write_sizes'][i],
                    'n_writes': conf['n_writes'][i],
                    'pattern': conf['patterns'][i],
                    'fsync': 1,
                    'sync': 0,
                    'file_path': os.path.join(self.conf['fs_mount_point'],
                        'multiwriter.file.' + str(i)),
                    'tag': 'tag' + str(i)
                    # 'markerfile': self.conf.get_blkparse_result_path()
                  }
            args_table.append(d)

        return args_table

    def run(self):
        args_table = self.translate(self.workload_conf)
        print args_table
        mw = multiwriters.MultiWriters("../wlgen/player-runtime",
                args_table)
        results = mw.run()

        for i, row in enumerate(results):
            results[i] = { 'player_'+k:v for k, v in row.items() }

        pprint.pprint(results)

        utils.table_to_file(results, os.path.join(self.conf['result_dir'],
            'multiwriters.results.txt'))
        print 'Written to disk'

    def stop(self):
        pass


class Simple(Workload):
    def run(self):
        utils.shcmd("sync")
        utils.shcmd("cp -r ./ {}".format(self.conf["fs_mount_point"]))
        utils.shcmd("rm -r {}/*".format(self.conf["fs_mount_point"]))
        utils.shcmd("sync")

    def stop(self):
        pass

class Mdtest(Workload):
    def run(self):
        self.conf['mdtest_settings']['running_dir'] = os.path.join(self.conf['fs_mount_point'], 'formdtest')
        cmd = 'mpirun -np {np} ./externals/mdtest/mdtest '\
            '-b {branches} -I {items_per_node} -z {depth} -d {running_dir} {create_only} '\
            '-w {write_bytes} {sync_write} -i {iterations}'\
            .format(**self.conf['mdtest_settings'])

        utils.shcmd(cmd)

    def stop(self):
        pass


class Tpcc(Workload):
    def start_mysql(self):
        utils.shcmd("sudo service mysql start")

    def stop_mysql(self):
        """You will get 'no instance found' if no mysql runningn"""
        # utils.shcmd("sudo service mysql stop")
        utils.shcmd("sudo stop mysql")


    def change_data_dir(self):
        """TODO: It has some hard-coded stuff"""
        utils.shcmd("cp -r /var/lib/mysql /mnt/fsonloop/")
        utils.shcmd("chown -R mysql:mysql /mnt/fsonloop/mysql")

        lines = []
        with open("/etc/mysql/my.cnf", "r") as f:
            for line in f:
                if line.startswith("datadir"):
                    line = "datadir     = /mnt/fsonloop/mysql\n"
                lines.append(line)

        with open("/etc/mysql/my.cnf", "w") as f:
            for line in lines:
                f.write(line)

        lines = []
        with open("/etc/apparmor.d/usr.sbin.mysqld", "r") as f:
            for line in f:
                line = line.replace('/var/lib/mysql', '/mnt/fsonloop/mysql')
                lines.append(line)

        with open("/etc/apparmor.d/usr.sbin.mysqld", "w") as f:
            for line in lines:
                f.write(line)

        utils.shcmd("cp -r /var/lib/mysql /mnt/fsonloop/")
        utils.shcmd("chown -R mysql:mysql /mnt/fsonloop/mysql")

    def execute_tpcc(self):
        self.change_data_dir()

        with utils.cd("../mysql-io-pattern/tpcc-mysql/tpcc-mysql"):
            # utils.shcmd("sudo mysqld &")
            # utils.shcmd("sudo /etc/init.d/mysql restart")
            # time.sleep(1)

            utils.shcmd("sudo service mysql restart")
            utils.shcmd('mysql -u root -p8888 -e "CREATE DATABASE tpcc1000;"')
            utils.shcmd('mysql -u root -p8888 tpcc1000 < create_table.sql')
            utils.shcmd('mysql -u root -p8888 tpcc1000 < add_fkey_idx.sql')

            utils.shcmd('./tpcc_load 127.0.0.1 tpcc1000 root "8888" 20')
            cmd = [
                    './tpcc_start',
                    '-h127.0.0.1',
                    '-dtpcc1000',
                    '-uroot',
                    '-p8888',
                    '-w20',
                    '-c16',
                    '-r10',
                    '-l1200'
                  ]
            utils.shcmd(" ".join(cmd))

    def run(self):
        try:
            self.execute_tpcc()
        finally:
            self.stop_mysql()


class Varmail(Workload):
    def get_conf_text(self, dirpath):
        part1 = """
set $dir={dirpath}
set $nfiles=8000
set $meandirwidth=1000000
set $filesize=cvar(type=cvar-gamma,parameters=mean:16384;gamma:1.5)
set $nthreads=16
set $iosize=1m
set $meanappendsize=16k
"""
        part2 = """
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

run 20
"""
        return part1.format(dirpath=dirpath) + part2

    def save_config(self, conf_text, inst_id):
        with open('/tmp/filebench.config.'+str(inst_id), 'w') as f:
            f.write(conf_text)

    def get_dirpath(self, inst_id):
        dirpath = os.path.join(self.conf['fs_mount_point'], 'varmail', str(inst_id))
        return dirpath

    def run_filebench(self, inst_id):
        cmd = 'filebench -f /tmp/filebench.config.' + str(inst_id)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()

    def run_inst(self, inst_id):
        dirpath = self.get_dirpath(inst_id)
        utils.prepare_dir(dirpath)
        conf_text = self.get_conf_text(dirpath)
        self.save_config(conf_text, inst_id)

        self.run_filebench(inst_id)

    def run(self):
        self.run_inst(0)


class Sqlite(Workload):
    def _execute_bench(self, n_insertions, pattern, inst_id, commit_period, max_key):
        bench_path = './sqlitebench/bench.py'
        db_dir = os.path.join(self.conf['fs_mount_point'], 'sqlite_dir')
        db_path = os.path.join(db_dir, 'inst-'+str(inst_id), 'data.db')

        utils.prepare_dir_for_path(db_path)

        cmd = 'python {exe} -f {f} -n {n} -p {p} -e {e} -m {m}'.format(
            exe=bench_path, f=db_path, n=n_insertions, p=pattern,
            e=commit_period, m=max_key)

        # cmd = "strace -o {}.strace.out -f -ttt -s 8 {}".format(inst_id, cmd)
        strace_prefix = ' '.join(['strace',
           '-o', str(inst_id) + '.strace.out', '-f', '-ttt',
           '-e', 'trace=open,openat,accept,close,fsync,sync,read,'\
                 'write,pread,pwrite,lseek,'\
                 'dup,dup2,dup3,clone,unlink',
           '-s', '8'])

        # cmd = strace_prefix + ' ' + cmd
        print cmd
        p = subprocess.Popen(cmd, shell=True)

        return p

    def run(self):
        benchconfs = self.workload_conf['benchconfs']

        procs = []
        for i, conf in enumerate(benchconfs):
            p = self._execute_bench(conf['n_insertions'], conf['pattern'], i,
                    conf['commit_period'], conf['max_key'])
            procs.append(p)

        for p in procs:
            p.wait()


class Leveldb(Workload):
    def parse_output(self, outputpath):
        outputpath_parsed = outputpath + '.parsed'
        tablestr = parse_file(outputpath)
        with open(outputpath_parsed, 'w') as f:
            f.write(tablestr)


    def _execute_leveldb(self, benchmarks, num, db, outputpath,
            threads, use_existing_db, max_key, max_log):
        utils.prepare_dir(db)

        db_bench_path = "../leveldb/db_bench"
        cmd = "{exe} --benchmarks={benchmarks} --num={num} --db={db} "\
                "--threads={threads}  "\
                "--dowrite_max_key={max_key} "\
                "--dowrite_skew_max_log={max_log} "\
                "--use_existing_db={use_existing_db} > {out}"\
            .format(exe=db_bench_path, benchmarks=benchmarks,
                num=num, db=db, out=outputpath,
                threads=threads,
                max_key=max_key,
                max_log=max_log,
                use_existing_db=use_existing_db
                )
        print cmd
        # cmd = "strace -o {}.strace.out -f {}".format(benchmarks, cmd)
        p = subprocess.Popen(cmd, shell=True)

        return p

    def run(self):
        data_dir = os.path.join(self.conf['fs_mount_point'], 'leveldb_data')
        outputpath = os.path.join(self.conf['result_dir'], 'leveldb.out')
        benchconfs = self.workload_conf['benchconfs']
        one_by_one = self.workload_conf['one_by_one']
        threads = self.workload_conf['threads']

        if one_by_one is True:
            for conf_dict in benchconfs:
                p = self._execute_leveldb(
                        benchmarks=conf_dict['benchmarks'],
                        threads=threads,
                        num=conf_dict['num'],
                        max_key=conf_dict['max_key'],
                        max_log=conf_dict['max_log'],
                        db=data_dir, outputpath=outputpath,
                        use_existing_db=1
                        )
                p.wait()
        else:
            procs = []
            for i, conf_dict in enumerate(benchconfs):
                p = self._execute_leveldb(
                        benchmarks=conf_dict['benchmarks'],
                        db=data_dir + str(i), outputpath=outputpath,
                        threads=threads,
                        num=conf_dict['num'],
                        max_key=conf_dict['max_key'],
                        max_log=conf_dict['max_log'],
                        use_existing_db=0
                        )
                procs.append(p)

            for p in procs:
                p.wait()



