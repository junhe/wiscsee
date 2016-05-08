import re
import time

import blocktrace
import config
import cpuhandler
import filesystem
import fshelper
import ftrace
from ssdbox import hostevent
from utilities import utils
import workload


class WorkloadRunner(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__()))
        self.conf = confobj

        # blktracer for making file system
        self.blktracer_mkfs = blocktrace.BlockTraceManager(
            confobj = self.conf,
            dev = self.conf['device_path'],
            resultpath = self.conf.get_blkparse_result_path_mkfs(),
            to_ftlsim_path = self.conf.get_ftlsim_events_output_path_mkfs(),
            sector_size = self.conf['sector_size'])

        # blktracer for running workload
        self.blktracer = blocktrace.BlockTraceManager(
            confobj = self.conf,
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
        self.workload = eval(workload_str)

    def prepare_device(self):

        if self.conf.device_type == 'loop':
            self.loopdev = filesystem.LoopDevice(
                dev_path = self.conf['device_path'],
                tmpfs_mount_point = self.conf['tmpfs_mount_point'],
                size_mb = self.conf['dev_size_mb'])
            self.loopdev.create()

        elif self.conf.device_type == 'real':
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
        # create file system object, it is not physically created
        fs = self.conf['filesystem']
        if fs == None:
            return

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
        fs_obj = fsclass(device = self.conf['device_path'],
            mount_point = self.conf['fs_mount_point'])

        # Making and mounting file system
        try:
            mk_opt_dic = self.conf[self.conf['filesystem']].get('make_opts', None)
        except KeyError:
            mk_opt_dic = None
        fs_obj.make(opt_dic = mk_opt_dic)
        fs_obj.mount(opt_list =
            self.conf['mnt_opts'][ self.conf['filesystem'] ])
        utils.shcmd('sync')

        # F2FS specific
        if self.conf['filesystem'] == 'f2fs':
            for opt_name, value in \
                self.conf['f2fs'].get('sysfs', {}).items():
                fs_obj.sysfs_setup(opt_name, value)

    def __set_linux_ncq_depth(self):
        if self.conf.device_type == 'real':
            device_name = self.conf.get_device_name_no_num()
            utils.set_linux_ncq_depth(device_name,
                self.conf['linux_ncq_depth'])

    def __set_linux_io_scheduler(self):
        if self.conf.device_type == 'real':
            device_name = self.conf.get_device_name_no_num()
            utils.set_linux_io_scheduler(device_name,
                self.conf['linux_io_scheduler'])

    def __set_linux_environment(self):
        self.__set_linux_ncq_depth()
        self.__set_linux_io_scheduler()

    def run(self):
        self.__set_linux_environment()

        if self.conf['enable_blktrace'] == True:
            return self.run_with_blktrace()
        else:
            return self.run_without_blktrace()

    def run_without_blktrace(self):
        cpuhandler.set_cpus(self.conf['n_online_cpus'])

        self.prepare_device()
        self.build_fs()

        self.aging_workload.run()

        self.workload.run()

        return None

    def run_with_blktrace(self):
        try:
            # Set number of CPUs
            cpuhandler.set_cpus(self.conf['n_online_cpus'])

            self.prepare_device()

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
            self.blktracer_mkfs.create_event_file_from_blkparse()

            self.blktracer.start_tracing_and_collecting()
            while self.blktracer.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

            print 'Running workload ....'
            self.workload.run()

        except Exception:
            raise
        else:
            self.blktracer.stop_tracing_and_collecting()
            utils.shcmd("sync")
            self.blktracer.create_event_file_from_blkparse()
            return self.get_event_iterator()
        finally:
            # always try to clean up the blktrace processes
            self.blktracer.stop_tracing_and_collecting()

    def get_event_iterator(self):
        yield hostevent.Event(sector_size = self.conf['sector_size'],
            pid = 0, operation = 'disable_recorder',
            offset = 0, size = 0)

        mkfs_iter = hostevent.FileLineIterator(
            self.conf.get_ftlsim_events_output_path_mkfs())
        event_mkfs_iter = hostevent.EventIterator(self.conf, mkfs_iter)

        for event in event_mkfs_iter:
            yield event

        # special event indicates the start of workload
        yield hostevent.Event(sector_size = self.conf['sector_size'],
            pid = 0, operation = 'enable_recorder',
            offset = 0, size = 0)
        yield hostevent.Event(sector_size = self.conf['sector_size'],
            pid = 0, operation = 'workloadstart',
            offset = 0, size = 0)

        workload_iter = hostevent.FileLineIterator(
            self.conf.get_ftlsim_events_output_path())
        event_workload_iter = hostevent.EventIterator(self.conf, workload_iter)

        for event in event_workload_iter:
            yield event


