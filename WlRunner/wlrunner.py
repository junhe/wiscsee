import re
import time

import blocktrace
import config
import cpuhandler
import filesystem
import fshelper
import ftrace
from FtlSim import simulator
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


class SimpleEventIterator(object):
    def __init__(self, filelineiter):
        self.filelineiter = filelineiter

    def str_to_event(self, line):
        """
        PID OPERATION OFFSET SIZE
        """
        items = line.split()
        return simulator.EventSimple(pid = items[0], operation = items[1])

    def __iter__(self):
        for line in self.filelineiter:
            yield self.str_to_event(line)


class EventIterator(object):
    def __init__(self, sector_size, filelineiter):
        self.sector_size = sector_size
        self.filelineiter = filelineiter

    def str_to_event(self, line):
        """
        PID OPERATION OFFSET SIZE
        """
        items = line.split()
        return simulator.Event(sector_size = self.sector_size,
                pid = items[0], operation = items[1], offset = items[2],
                size = items[3])

    def __iter__(self):
        for line in self.filelineiter:
            yield self.str_to_event(line)



class WorkloadRunner(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__()))
        self.conf = confobj

        if self.conf['device_type'] == 'loop':
            # create loop dev object, it is not physically created yet
            self.loopdev = filesystem.LoopDevice(
                dev_path = self.conf['device_path'],
                tmpfs_mount_point = self.conf['tmpfs_mount_point'],
                size_mb = self.conf['dev_size_mb'])

        # create file system object, it is not physically created
        fs = self.conf['filesystem']
        if fs == 'ext4':
            fsclass = filesystem.Ext4
        elif fs == 'f2fs':
            fsclass = filesystem.F2fs
        elif fs == 'btrfs':
            fsclass = filesystem.Btrfs
        elif fs == 'xfs':
            fsclass = filesystem.Xfs
        else:
            raise RuntimeError("{} is not a valid file system type"\
                .format(fs))
        self.fs = fsclass(device = self.conf['device_path'],
            mount_point = self.conf['fs_mount_point'])

        # blktracer for making file system
        self.blktracer_mkfs = blocktrace.BlockTraceManager(
            dev = self.conf['device_path'],
            resultpath = self.conf.get_blkparse_result_path_mkfs(),
            to_ftlsim_path = self.conf.get_ftlsim_events_output_path_mkfs(),
            sector_size = self.conf['sector_size'])

        # blktracer for running workload
        self.blktracer = blocktrace.BlockTraceManager(
            dev = self.conf['device_path'],
            resultpath = self.conf.get_blkparse_result_path(),
            to_ftlsim_path = self.conf.get_ftlsim_events_output_path(),
            sector_size = self.conf['sector_size'])

        # create aging workload object
        self.aging_workload = eval("workload.{wlclass}(confobj = self.conf, " \
            "workload_conf_key = '{wlconf_key}')".format(
                wlclass = self.conf["age_workload_class"],
                wlconf_key = self.conf["aging_config_key"]
                ) #format
            ) #eval

        # create workload object
        workload_str = "workload.{wlclass}(confobj = self.conf, " \
            "workload_conf_key = '{wlconf_key}')".format(
                wlclass = self.conf["workload_class"],
                wlconf_key = self.conf["workload_conf_key"]
                ) #format
        print workload_str
        self.workload = eval(workload_str)

        # create Ftrace manager
        # self.ftrace = ftrace.Ftrace()

    def prepare_fs(self):
        # Prepare file systems
        if self.conf['device_type'] == 'loop':
            self.loopdev.create()
        elif self.conf['device_type'] == 'real':
            # umount file system if it is mounted
            if fshelper.isMounted(self.conf['fs_mount_point']):
                utils.shcmd(
                    "sudo umount {}".format(self.conf['fs_mount_point']))
            # partition the dev
            base_dev_path = self.conf['device_path'].rstrip('0123456789')

            mo = re.search(r'\d+$', self.conf['device_path'])
            if mo == None:
                raise RuntimeError("You have to specify a partition, not "
                        "an entire disk: {}".format(
                        self.conf['device_path']))
            dev_id = int(mo.group())
            # if dev_id = 3, we will have:
            #    [0 0 0]
            # sdc 1 2 3
            part_sizes = [0 for i in range(dev_id)]
            size = self.conf['dev_size_mb'] * 2**20
            part_sizes[dev_id - 1] = size
            fshelper.partition_disk(base_dev_path, part_sizes)

    def build_fs(self):
        # Making and mounting file system
        try:
            mk_opt_dic = self.conf[self.conf['filesystem']].get('make_opts', None)
        except KeyError:
            mk_opt_dic = None
        self.fs.make(opt_dic = mk_opt_dic)
        self.fs.mount(opt_list =
            self.conf['mnt_opts'][ self.conf['filesystem'] ])
        utils.shcmd('sync')

        # F2FS specific
        if self.conf['filesystem'] == 'f2fs':
            for opt_name, value in \
                self.conf['f2fs'].get('sysfs', {}).items():
                self.fs.sysfs_setup(opt_name, value)

    def run(self):
        if self.conf['enable_blktrace'] == True:
            return self.run_with_blktrace()
        else:
            return self.run_without_blktrace()

    def run_without_blktrace(self):
        cpuhandler.set_cpus(self.conf['n_online_cpus'])

        self.prepare_fs()
        self.build_fs()

        self.aging_workload.run()

        self.workload.run()

        return None

    def run_with_blktrace(self):
        try:
            # Set number of CPUs
            cpuhandler.set_cpus(self.conf['n_online_cpus'])

            self.prepare_fs()

            # strat blktrace
            # This is only for making and mounting file system, because we
            # want to separate them with workloads.
            self.blktracer_mkfs.start_tracing_and_collecting()
            time.sleep(0.5)
            while self.blktracer_mkfs.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

            self.build_fs()

            # Age the file system
            self.aging_workload.run()

            time.sleep(1)
            self.blktracer_mkfs.stop_tracing_and_collecting()
            time.sleep(1)
            self.blktracer_mkfs.blkparse_file_to_ftlsim_input_file()

            self.blktracer.start_tracing_and_collecting()
            time.sleep(1)
            while self.blktracer.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

            print 'Running workload ....'
            self.workload.run()

        except Exception:
            raise
        else:
            time.sleep(1)
            self.blktracer.stop_tracing_and_collecting()
            utils.shcmd("sync")
            self.blktracer.blkparse_file_to_ftlsim_input_file()
            return self.get_event_iterator()
        finally:
            # always try to clean up the blktrace processes
            self.blktracer.stop_tracing_and_collecting()

    def get_event_iterator(self):
        yield simulator.Event(sector_size = self.conf['sector_size'],
            pid = 0, operation = 'disable_recorder',
            offset = 0, size = 0)

        mkfs_iter = FileLineIterator(
            self.conf.get_ftlsim_events_output_path_mkfs())
        event_mkfs_iter = EventIterator(self.conf['sector_size'],
                mkfs_iter)

        for event in event_mkfs_iter:
            yield event

        # special event indicates the start of workload
        yield simulator.Event(sector_size = self.conf['sector_size'],
            pid = 0, operation = 'enable_recorder',
            offset = 0, size = 0)
        yield simulator.Event(sector_size = self.conf['sector_size'],
            pid = 0, operation = 'workloadstart',
            offset = 0, size = 0)

        workload_iter = FileLineIterator(
            self.conf.get_ftlsim_events_output_path())
        event_workload_iter = EventIterator(self.conf['sector_size'],
            workload_iter)

        for event in event_workload_iter:
            yield event


