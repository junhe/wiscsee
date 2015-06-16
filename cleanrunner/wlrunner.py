import blocktrace
import config
import filesystem
import workload

class WorkloadRunner(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config")
        self.conf = confobj

        # create loop dev object, it is not physically created
        self.loopdev = filesystem.LoopDevice(
            dev_path = self.conf['loop_path'],
            tmpfs_mount_point = self.conf['tmpfs_mount_point'],
            size_mb = self.conf['loop_dev_size_mb'])

        # create file system object, it is not physically created
        fs = self.conf['filesystem']
        if fs == 'ext4':
            fsclass = filesystem.Ext4
        elif fs == 'f2fs':
            fsclass = filesystem.F2fs
        else:
            raise RuntimeError("{} is not a valid file system type"\
                .format(fs))
        self.fs = fsclass(device = self.conf['loop_path'],
            mount_point = self.conf['fs_mount_point'])

        # create blktrace manager object
        self.blktracer = blocktrace.BlockTraceManager(
            dev = self.conf['loop_path'],
            resultpath = self.conf.get_blkparse_result_path(),
            to_ftlsim_path = self.conf.get_ftlsim_events_output_path(),
            sector_size = self.conf['sector_size'])

        # create workload object
        self.workload = eval("workload.{}(self.conf)".format(
            self.conf["workload_class"]))

    def run(self):
        try:
            self.loopdev.create()
            self.blktracer.start_tracing_and_collecting()
            self.fs.make()
            self.fs.mount()
            self.workload.run()
        except Exception:
            raise
        else:
            self.blktracer.blkparse_file_to_ftlsim_input_file()
            print 'file wrote to {}'.format(
                self.conf.get_ftlsim_events_output_path())
        finally:
            # always try to clean up the blktrace processes
            self.blktracer.stop_tracing_and_collecting()

    def get_iterator(self):
        pass


