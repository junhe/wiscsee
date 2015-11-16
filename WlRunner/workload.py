import os
import random
import time

import config
import utils
import workloadlist

class Workload(object):
    def __init__(self, confobj, workload_conf = None):
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
        self.workload_conf = workload_conf

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
    def start_mysql():
        shcmd("sudo service mysql start")

    def stop_mysql():
        """You will get 'no instance found' if no mysql runningn"""
        shcmd("sudo service mysql stop")


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

    def run(self):
        try:
            self.stop_mysql()
        except Exception:
            pass

        self.change_data_dir()

        with utils.cd("/home/jun/workdir/mysql-io-pattern/tpcc-mysql/tpcc-mysql"):
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

    def stop(self):
        utils.shcmd("sudo service mysql stop")


class Sqlbench(Workload):
    def start_mysql():
        shcmd("sudo service mysql start")

    def stop_mysql():
        """You will get 'no instance found' if no mysql runningn"""
        shcmd("sudo service mysql stop")


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
        try:
            self.stop_mysql()
        except Exception:
            pass

        try:
            self.change_data_dir()

            utils.shcmd("sudo service mysql restart")
            utils.shcmd("mysqladmin -u root -p8888 create test")


            for test in self.conf['sqlbench']['benches_to_run']:
                self.run_sqlbench(test)
        finally:
            try:
                self.stop_mysql()
            except Exception:
                pass

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
        for rep in range(setting['iterations']):
            for i in range(0, chunkcnt):
                for fileid in fileids:
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
        for rep in range(setting['iterations']):
            for i in range(0, chunkcnt):
                for fileid in fileids:
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

        utils.shcmd("mpirun -np {np} ../wlgen/player {workload}"\
            .format(np = wllist.max_pid + 1, workload = tmppath))
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

