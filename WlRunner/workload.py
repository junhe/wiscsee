import config
import utils

class Workload(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config")
        self.conf = confobj

    def run(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class Simple(Workload):
    def run(self):
        utils.shcmd("sync")
        utils.shcmd("cp -r /boot {}".format(self.conf["fs_mount_point"]))
        utils.shcmd("rm -r {}/*".format(self.conf["fs_mount_point"]))
        utils.shcmd("sync")

    def stop(self):
        pass



