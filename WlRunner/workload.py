import os
import random
import time

import config
import utils
import workloadlist

class Workload(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__))

        self.conf = confobj

    def run(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


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
    def generate_sequential_workload(self):
        setting = self.conf['Synthetic']

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepath = 'testfile'
        wllist.add_call(name='open', pid=0, path=filepath)

        for rep in range(setting['iterations']):
            for i in range(setting['chunk_count']):
                offset = setting['chunk_size'] * i
                size = setting['chunk_size']
                wllist.add_call(name='write', pid=0, path=filepath,
                    offset=offset, count=size)
            wllist.add_call(name='fsync', pid=0, path=filepath)

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
        setting = self.conf['Synthetic']

        wllist = workloadlist.WorkloadList(self.conf['fs_mount_point'])
        filepath = 'testfile'
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

    def run_by(self, generator_func):
        wllist = generator_func()
        tmppath = '/tmp/tmp_workloadfile'
        wllist.save(tmppath)

        utils.shcmd("mpirun -np 1 ../wlgen/player {}".format(tmppath))
        utils.shcmd("sync")

    def run(self):
        # self.run_by(self.generate_sequential_workload)
        # self.run_by(self.generate_backward_workload)
        eval("self.run_by({})".format(
            self.conf['Synthetic']['generating_func']))

    def stop(self):
        pass


if __name__ == '__main__':
    # tpcc = Tpcc()
    # tpcc.run()
    # tpcc.stop()

    sqlbench = Sqlbench()
    sqlbench.run()

