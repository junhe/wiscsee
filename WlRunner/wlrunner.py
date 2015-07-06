import time

import blocktrace
import config
import filesystem
import ftrace
import utils
import workload

class FileLineIterator(object):
    def __init__(self, file_path):
        self.file_path = file_path

    def __iter__(self):
        with open(self.file_path, 'r') as f:
            for line in f:
                line = line.strip()
                yield line

class WorkloadRunner(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__()))
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
        elif fs == 'btrfs':
            fsclass = filesystem.Btrfs
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

        # create Ftrace manager
        # self.ftrace = ftrace.Ftrace()

    def run(self):
        try:
            # Prepare file systems
            self.loopdev.create()

            # strat blktrace
            # You have to start blktrace before making file system
            # Otherwise, the making and mounting will NOT be simulated
            # by FtlSim
            self.blktracer.start_tracing_and_collecting()
            while self.blktracer.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

            self.fs.make()
            self.fs.mount(opt_list=self.conf['common_mnt_opts'])
            utils.shcmd('sync')



            # start Ftrace
            # self.ftrace.set_filter('*f2fs*')
            # self.ftrace.start_tracing()
            # self.ftrace.clean_trace()
            # self.ftrace.write_marker('JUN: beginning of workload..............')
            # self.ftrace.run_stats()

            self.workload.run()

            utils.shcmd('sync')
        except Exception:
            raise
        else:
            # finish Ftrace
            # self.ftrace.write_marker('JUN: end of workload..............')
            # print "trying to stop stats"
            # self.ftrace.write_marker('send to pipe.')
            # self.ftrace.stop_stats()
            # self.ftrace.write_marker('send to pipe.')

            self.blktracer.blkparse_file_to_ftlsim_input_file()
            print 'file wrote to {}'.format(
                self.conf.get_ftlsim_events_output_path())
            return self.get_event_iterator()
        finally:
            # always try to clean up the blktrace processes
            self.blktracer.stop_tracing_and_collecting()

    def get_event_iterator(self):
        return FileLineIterator(self.conf.get_ftlsim_events_output_path())

