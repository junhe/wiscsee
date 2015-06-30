import os
import time

import config
import utils

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
    def run(self):
        print ['sssssss'] * 100
        utils.shcmd("mkdir /mnt/fsonloop/mysql")
        with utils.cd("/home/jun/workdir/mysql-io-pattern/tpcc-mysql/tpcc-mysql"):
            # utils.shcmd("sudo mysqld &")
            # utils.shcmd("sudo /etc/init.d/mysql restart")
            # time.sleep(1)
            utils.shcmd('./tpcc_load 127.0.0.1 tpcc1000 root "8888" 20')
            # cmd = [
                    # './tpcc_start',
                    # '-h127.0.0.1',
                    # '-dtpcc1000',
                    # '-uroot',
                    # '-p8888',
                    # '-w20',
                    # '-c16',
                    # '-r10',
                    # '-l1200'
                  # ]
            # utils.shcmd(" ".join(cmd))

    def stop(self):
        utils.shcmd("sudo pkill mysqld")

if __name__ == '__main__':
    tpcc = Tpcc()
    tpcc.run()
    tpcc.stop()

