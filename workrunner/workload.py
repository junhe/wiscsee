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

from accpatterns import patterns
from .patternonfile import File
import patternsuite

try:
    parent = os.path.join(sys.path[0], '../reuse/')
    sys.path.append(parent)
    import pyreuse
    from pyreuse.sysutils.ftrace import trace_cmd
    from pyreuse.apputils.parseleveldboutput import parse_file
except ImportError:
    print "Failed to import pyreuse, but it may be ok"

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


class PatternSuite(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(PatternSuite, self).__init__(confobj, workload_conf_key)


    def _get_iter(self):
        patternname = self.workload_conf['patternname']
        patterncls = eval('patternsuite.'+patternname)
        req_iter = patterncls(**self.workload_conf['parameters'])
        return req_iter

    def run(self):
        datapath = os.path.join(self.conf["fs_mount_point"], 'datafile')

        # req_iter = patterns.Random(op=patterns.WRITE, zone_offset=0, zone_size=8*MB,
                # chunk_size=1*MB, traffic_size=1*MB)
        req_iter = self._get_iter()

        f = File(datapath)
        f.open()
        f.access(req_iter)
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



class Leveldb(Workload):
    def parse_output(self, outputpath):
        outputpath_parsed = outputpath + '.parsed'
        tablestr = parse_file(outputpath)
        with open(outputpath_parsed, 'w') as f:
            f.write(tablestr)


    def _execute_leveldb(self, benchmarks, num, db, outputpath,
            threads=0,
            use_existing_db=1):
        utils.prepare_dir(db)

        db_bench_path = "../leveldb-1.18/db_bench"
        cmd = "{exe} --benchmarks={benchmarks} --num={num} --db={db} "\
                "--threads={threads} "\
                "--use_existing_db={use_existing_db} > {out}"\
            .format(exe=db_bench_path, benchmarks=benchmarks,
                num=num, db=db, out=outputpath,
                threads=threads,
                use_existing_db=use_existing_db
                )
        print cmd
        p = subprocess.Popen(cmd, shell=True)

        return p

    def run(self):
        data_dir = os.path.join(self.conf['fs_mount_point'], 'leveldb_data')
        outputpath = os.path.join(self.conf['result_dir'], 'leveldb.out')
        benchmarks = self.workload_conf['benchmarks']
        num = self.workload_conf['num']
        one_by_one = self.workload_conf['one_by_one']
        pre_run_kv_num = self.workload_conf['pre_run_kv_num']
        threads = self.workload_conf['threads']

        n_instances = self.workload_conf['n_instances']

        if one_by_one is True:
            p = self._execute_leveldb(benchmarks='fillrandom',
                    num=pre_run_kv_num,
                    threads=threads,
                    db=data_dir, outputpath=outputpath,
                    use_existing_db=0
                    )
            p.wait()
            for i in range(n_instances):
                p = self._execute_leveldb(benchmarks=benchmarks, num=num,
                        threads=threads,
                        db=data_dir, outputpath=outputpath,
                        use_existing_db=1
                        )
                p.wait()
        else:
            procs = []
            for i in range(n_instances):
                p = self._execute_leveldb(benchmarks=benchmarks, num=num,
                        db=data_dir + str(i), outputpath=outputpath,
                        threads=threads,
                        use_existing_db=0
                        )
                procs.append(p)

            for p in procs:
                p.wait()


class Sqlbench(Workload):
    def start_mysql(self):
        utils.shcmd("sudo service mysql start")

    def stop_mysql(self):
        """You will get 'no instance found' if no mysql runningn"""
        print 'stop_mysql() ---------------------~~~~'
        utils.shcmd("sudo stop mysql")
        # shcmd("sudo service mysql stop")

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


    def run_sqlbench(self, testname):
        print 'in run_sqlbench***********'
        with utils.cd('../mysql-io-pattern/bench/sql-bench/'):
            cmd = ['perl',
                    testname,
                    '--user', 'root',
                    '--password', '8888'
                    ]
            # subprocess.call(cmd)
            utils.shcmd(' '.join(cmd))

    def run(self):
        # self.stop_mysql()

        try:
            self.change_data_dir()

            utils.shcmd("sudo service mysql restart")
            # utils.shcmd("mysqladmin -u root -p8888 create test")

            test = self.conf['sqlbench']['bench_to_run']
            self.run_sqlbench(test)

        finally:
            self.stop_mysql()

    def stop(self):
        utils.shcmd("sudo service mysql stop")


class Synthetic(Workload):
    def generate_mix_seq_workload(self):
        """
        This workload writes two files alternatively and sequentially, and
        then delete half of them
        """
        setting = self.workload_conf
        nfiles = setting['num_files']

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepaths = ['file.'+str(i) for i in range(nfiles)]

        for filei in range(nfiles):
            wllist.add_call(name='open', pid=0, path=filepaths[filei])

        for _ in range(setting['iterations']):
            for i in range(setting['chunk_count']):
                for filei in range(nfiles):
                    offset = setting['chunk_size'] * i
                    size = setting['chunk_size']
                    wllist.add_call(name='write', pid=0, path=filepaths[filei],
                        offset=offset, count=size)
                    # wllist.add_call(name='fsync', pid=0, path=filepaths[filei])
            wllist.add_call(name='sync', pid=0)

        for filei in range(nfiles):
            wllist.add_call(name='close', pid=0, path=filepaths[filei])

        for filei in range(nfiles):
            if filei % 2 == 0:
                wllist.add_call(name='rm', pid=0, path=filepaths[filei])
        return wllist


    def generate_sequential_workload(self):
        setting = self.workload_conf

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepath = setting['filename']
        wllist.add_call(name='open', pid=0, path=filepath)

        for rep in range(setting['iterations']):
            for i in range(setting['chunk_count']):
                offset = setting['chunk_size'] * i
                size = setting['chunk_size']
                wllist.add_call(name='write', pid=0, path=filepath,
                    offset=offset, count=size)
            # wllist.add_call(name='fsync', pid=0, path=filepath)
            wllist.add_call(name='sync', pid=0)

        wllist.add_call(name='close', pid=0, path=filepath)
        return wllist

    def generate_backward_workload(self):
        setting = self.conf['Synthetic']

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepath = 'testfile'
        wllist.add_call(name='open', pid=0, path=filepath)

        for rep in range(setting['iterations']):
            for i in reversed(range(setting['chunk_count'])):
                offset = setting['chunk_size'] * i
                size = setting['chunk_size']
                wllist.add_call(name='write', pid=0, path=filepath,
                    offset=offset, count=size)
                wllist.add_call(name='fsync', pid=0, path=filepath)

        wllist.add_call(name='close', pid=0, path=filepath)
        return wllist

    def generate_random_workload(self):
        # setting = self.conf['Synthetic']
        setting = self.workload_conf

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepath = setting['filename']
        wllist.add_call(name='open', pid=0, path=filepath)

        random.seed(1)
        # random_seq = range(setting['chunk_count'])
        chunkcnt = setting['chunk_count']

        for rep in range(setting['iterations']):
            for i in range(0, chunkcnt):
                offset = setting['chunk_size'] * random.randint(0, chunkcnt)
                size = setting['chunk_size']
                wllist.add_call(name='write', pid=0, path=filepath,
                    offset=offset, count=size)
                wllist.add_call(name='fsync', pid=0, path=filepath)

        wllist.add_call(name='close', pid=0, path=filepath)
        return wllist

    def generate_serial_random_writes(self):
        """
        Counterpart of parallel random writes, only difference is that
        this one write one file after another. The order of chunks is
        the same.
        """
        setting = self.workload_conf

        nfiles = setting['nfiles']

        fileids = range(nfiles)
        filepaths = [ setting['filename'] + str(i) for i in fileids ]

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])

        random.seed(1)
        chunkcnt = setting['chunk_count']

        # store the random chunk sequence for each file
        # each file is accessed in the sequence of popping
        rand_seqs = {}
        for fileid in fileids:
            for rep in range(setting['iterations']):
                for i in range(0, chunkcnt):
                    if not rand_seqs.has_key(fileid):
                        rand_seqs[fileid] = []
                    rand_seqs[fileid].append(random.randint(0, chunkcnt))

        for fileid in fileids:
            # do the files one by one
            filepath = filepaths[fileid]
            wllist.add_call(name='open', pid=fileid, path=filepath)

            for rep in range(setting['iterations']):
                for i in range(0, chunkcnt):
                    filepath = filepaths[fileid]
                    offset = setting['chunk_size'] *  rand_seqs[fileid].pop()
                    size = setting['chunk_size']
                    wllist.add_call(name='write', pid=fileid, path=filepath,
                        offset=offset, count=size)
                    wllist.add_call(name='fsync', pid=fileid, path=filepath)

            # close
            wllist.add_call(name='close', pid=fileid, path=filepath)

        return wllist

    def generate_parallel_writes(self):
        """
        You will be able to tune
        - file size of each file.
        - number of files
        - sequential or random
        - chunk size, each file will have the same number of chunks, having larger
        chunk size will invalidate a file quicker (thus hotter).

        example setting:
        { filesizes: [1MB, 2MB],
          pattern:   ['sequential', 'random'],
          chunksize: [16KB, 32KB]
          }

        You will need to use file size and chunk count to find chunk size
        """
        setting = self.workload_conf

        filesizes = setting['filesizes']
        nfiles = len(filesizes)
        fileids = range(nfiles)
        filepaths = [ setting['filename'] + str(i) for i in fileids ]

        patterns = setting['patterns']
        chunksizes = setting['chunk_sizes']
        # note writes_per_iter * chunk_size may not =filesize
        writes_per_iter = setting['writes_per_iter']
        chunkcnts = [ filesizes[fileid] / chunksizes[fileid] for fileid in fileids ]

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])

        # open
        for fileid in fileids:
            filepath = filepaths[fileid]
            wllist.add_call(name='open', pid=fileid, path=filepath)

        random.seed(1)

        # store the random chunk sequence for each file
        # each file is accessed in the sequence of popping
        rand_seqs = {}
        for fileid in fileids:
            per_file_total = 0
            for rep in range(setting['iterations']):
                for i in range(0, writes_per_iter):
                    if not rand_seqs.has_key(fileid):
                        rand_seqs[fileid] = collections.deque()

                    if patterns[fileid] == 'sequential':
                        rand_seqs[fileid].append(per_file_total % chunkcnts[fileid])
                        per_file_total += 1
                    elif patterns[fileid] == 'random':
                        rand_seqs[fileid].append(random.randint(0, chunkcnts[fileid]))
                    else:
                        raise NotImplementedError()

        for rep in range(setting['iterations']):
            for i in range(0, writes_per_iter):
                for fileid in fileids:
                    filepath = filepaths[fileid]
                    offset = chunksizes[fileid] * rand_seqs[fileid].popleft()
                    size = chunksizes[fileid]
                    wllist.add_call(name='write', pid=fileid, path=filepath,
                        offset=offset, count=size)
                    wllist.add_call(name='fsync', pid=fileid, path=filepath)

        # close
        for fileid in fileids:
            filepath = filepaths[fileid]
            wllist.add_call(name='close', pid=fileid, path=filepath)

        return wllist


    def generate_parallel_random_writes(self):
        setting = self.workload_conf

        nfiles = setting['nfiles']

        fileids = range(nfiles)
        filepaths = [ setting['filename'] + str(i) for i in fileids ]

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])

        # open
        for fileid in fileids:
            filepath = filepaths[fileid]
            wllist.add_call(name='open', pid=fileid, path=filepath)

        random.seed(1)
        chunkcnt = setting['chunk_count']

        # store the random chunk sequence for each file
        # each file is accessed in the sequence of popping
        rand_seqs = {}
        for fileid in fileids:
            for rep in range(setting['iterations']):
                for i in range(0, chunkcnt):
                    if not rand_seqs.has_key(fileid):
                        rand_seqs[fileid] = []
                    rand_seqs[fileid].append(random.randint(0, chunkcnt))

        for rep in range(setting['iterations']):
            for i in range(0, chunkcnt):
                for fileid in fileids:
                    filepath = filepaths[fileid]
                    offset = setting['chunk_size'] * rand_seqs[fileid].pop()
                    size = setting['chunk_size']
                    wllist.add_call(name='write', pid=fileid, path=filepath,
                        offset=offset, count=size)
                    wllist.add_call(name='fsync', pid=fileid, path=filepath)

        # close
        for fileid in fileids:
            filepath = filepaths[fileid]
            wllist.add_call(name='close', pid=fileid, path=filepath)

        return wllist

    def generate_hotcold_workload(self):
        def write_chunk(chunk_id):
            offset = setting['chunk_size'] * chunk_id
            size = setting['chunk_size']
            wllist.add_call(name='write', pid=0, path=filepath,
                offset=offset, count=size)
            wllist.add_call(name='fsync', pid=0, path=filepath)

        setting = self.conf['Synthetic']

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepath = 'testfile'
        wllist.add_call(name='open', pid=0, path=filepath)

        chunkcnt = setting['chunk_count']

        for rep in range(setting['iterations']):
            for i in Bricks(setting['n_col'], chunkcnt):
                write_chunk(i)

        wllist.add_call(name='close', pid=0, path=filepath)
        return wllist

    def run_by(self, generator_func):
        wllist = generator_func()
        tmppath = '/tmp/tmp_workloadfile'
        wllist.save(tmppath)

        cmd = "mpirun -np {np} ../wlgen/player {workload}"\
            .format(np = wllist.max_pid + 1, workload = tmppath)
        # utils.shcmd("mpirun -np {np} ../wlgen/player {workload}"\
            # .format(np = wllist.max_pid + 1, workload = tmppath))
        perf_text = utils.run_and_get_output(cmd)
        print '\n'.join(perf_text)
        duration = float(perf_text[-1].split()[-1])

        path = os.path.join(self.conf['result_dir'], 'workloadrun-duratio.txt')
        with open(path, 'w') as f:
            f.write(str(duration))

        utils.shcmd("sync")

    def run(self):
        # self.run_by(self.generate_sequential_workload)
        # self.run_by(self.generate_backward_workload)

        eval("self.run_by({})".format(
            self.workload_conf['generating_func']))

    def stop(self):
        pass

class Bricks(object):
    def __init__(self, n_col, n_units):
        self.n_col = n_col
        self.n_units = n_units

    def __iter__(self):
        it = bricks(self.n_col, self.n_units)
        for item in it:
            yield item

def bricks(n_col, n_units):
    units_per_col = n_units / n_col
    assert n_units % n_col == 0

    n_row = n_col

    for row in range(n_row):
        for col in range(row+1):
            unit_start = col * units_per_col
            unit_end = (col + 1) * units_per_col
            for unit_id in range(unit_start, unit_end):
                yield unit_id

if __name__ == '__main__':
    # tpcc = Tpcc()
    # tpcc.run()
    # tpcc.stop()

    # sqlbench = Sqlbench()
    # sqlbench.run()

    for j in range(2):
        for i in Bricks(2, 4):
            print i

