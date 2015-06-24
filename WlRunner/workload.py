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
    def runmdtest(self, **kwargs):
        "kwargs will be expanded in format()"
        kwargs['running_dir'] = self.conf['fs_mount_point']
        cmd = 'mpirun -np {np} ./externals/mdtest/mdtest '\
            '-b {branches} -I {items_per_node} -z {depth} -d {running_dir}'\
            .format(**kwargs)
            # .format(np=1, branches=3, items_per_node=1, depth=1,
            # running_dir='/tmp/mytmp')
        utils.shcmd(cmd)

    def run(self):
        self.conf['mdtest_settings']['running_dir'] = self.conf['fs_mount_point']
        cmd = 'mpirun -np {np} ./externals/mdtest/mdtest '\
            '-b {branches} -I {items_per_node} -z {depth} -d {running_dir}'\
            .format(**self.conf['mdtest_settings'])

        utils.shcmd(cmd)

    def stop(self):
        pass

