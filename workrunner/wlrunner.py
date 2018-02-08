import re
import os
import time
import datetime

from pyreuse.sysutils import blocktrace
from pyreuse.fsutils.ext4dumpextents import get_extents_of_dir
import config
import cpuhandler
import filesystem
import fshelper
from wiscsim import hostevent
from utilities import utils
import workload

from commons import *


class BarrierGen(object):
    def __init__(self, n_ncq_slots):
        self.n_ncq_slots = n_ncq_slots

    def barrier_events(self):
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)
        yield hostevent.ControlEvent(operation=OP_BARRIER)
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)


class WorkloadRunner(object):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__()))
        self.conf = confobj

        if self.conf.device_type == 'loop':
            # we don't pad loop
            self.conf['dev_padding'] = 0

        # blktracer for making file system
        self.blktracer_prepfs = blocktrace.BlockTraceManager(
            dev = self.conf['device_path'],
            event_file_column_names =  self.conf['event_file_column_names'],
            resultpath = self.conf.get_blkparse_result_path_mkfs(),
            to_ftlsim_path = self.conf.get_ftlsim_events_output_path_mkfs(),
            sector_size = self.conf['sector_size'],
            padding_bytes = self.conf['dev_padding'],
            do_sort = self.conf['sort_block_trace']
            )

        # blktracer for running workload
        self.blktracer = blocktrace.BlockTraceManager(
            dev = self.conf['device_path'],
            event_file_column_names =  self.conf['event_file_column_names'],
            resultpath = self.conf.get_blkparse_result_path(),
            to_ftlsim_path = self.conf.get_ftlsim_events_output_path(),
            sector_size = self.conf['sector_size'],
            padding_bytes = self.conf['dev_padding'],
            do_sort = self.conf['sort_block_trace']
            )

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
            self.conf['dev_padding'] = 0

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

            if dev_id == 0:
                raise RuntimeError(
                        "Disk partition ID (a part of device_path) starts with 1. "
                        "Example: /dev/sdc1.")
            if dev_id > 4:
                raise RuntimeError(
                        "Only 4 partitions allowed. We will format the whole disk. "
                        "You probably should just specify the first "
                        "partition of your disk in device_path."
                        )

            # if dev_id = 3, we will have:
            #    [0 0 0]
            # sdc 1 2 3
            part_sizes = [0 for i in range(dev_id)]
            size = self.conf['dev_size_mb'] * 2**20
            part_sizes[dev_id - 1] = size
            fshelper.partition_disk(base_dev_path, part_sizes,
                    self.conf['dev_padding']
                    )

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

        print '----------------------------------------------------'
        print '---------Running Aging Workload_-------------------'
        print '----------------------------------------------------'

        self.aging_workload.run()
        utils.drop_caches()

        self._pre_target_workload()

        start_time = datetime.datetime.now()
        self.workload.run()
        end_time = datetime.datetime.now()

        app_duration = end_time - start_time
        print 'Application duration >>>>>>>>>', app_duration.total_seconds()
        self.write_app_duration(app_duration.total_seconds())

        self._post_target_workload()

        return None

    def run_with_blktrace(self):
        try:
            # Set number of CPUs
            cpuhandler.set_cpus(self.conf['n_online_cpus'])

            self.prepare_device()

            # strat blktrace
            # This is only for making and mounting file system, because we
            # want to separate them with workloads.
            if self.conf['trace_issue_and_complete'] is True:
                trace_filter=['issue', 'complete']
            else:
                trace_filter=['issue']

            self.blktracer_prepfs.start_tracing_and_collecting(trace_filter=trace_filter)
            time.sleep(1)
            while self.blktracer_prepfs.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

            self.build_fs()

            # Age the file system
            print '----------------------------------------------------'
            print '---------Running Aging Workload-------------------'
            print '----------------------------------------------------'
            self.aging_workload.run()
            utils.drop_caches()

            time.sleep(1)
            self.blktracer_prepfs.stop_tracing_and_collecting()
            time.sleep(1)
            self.blktracer_prepfs.create_event_file_from_blkparse()

            self.blktracer.start_tracing_and_collecting(trace_filter=trace_filter)

            time.sleep(2)
            while self.blktracer.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

            print 'Running workload ..................'
            self._pre_target_workload()

            print '----------------------------------------------------'
            print '---------Running       TARGET workload-------------------'
            print '----------------------------------------------------'
            start_time = datetime.datetime.now()
            self.workload.run()
            end_time = datetime.datetime.now()

            app_duration = end_time - start_time
            print 'Application duration >>>>>>>>>', app_duration.total_seconds()
            self.write_app_duration(app_duration.total_seconds())

            self._post_target_workload()
            time.sleep(1) # has to sleep here so the blktrace gets all the data

        except Exception:
            raise
        else:
            utils.shcmd("sync")
            self.blktracer.stop_tracing_and_collecting()
            utils.shcmd("sync")
            self.blktracer.create_event_file_from_blkparse()
            # self.remove_raw_trace()
            return self.get_event_iterator()
        finally:
            # always try to clean up the blktrace processes
            self.blktracer.stop_tracing_and_collecting()

    def write_app_duration(self, secs):
        path = os.path.join(self.conf['result_dir'], 'app_duration.txt')
        with open(path, 'w') as f:
            f.write(str(secs))

    def remove_raw_trace(self):
        os.remove(self.conf.get_blkparse_result_path_mkfs())
        os.remove(self.conf.get_blkparse_result_path())

    def _pre_target_workload(self):
        pass

    def _post_target_workload(self):
        if self.conf.get('do_fstrim', False) is True:
            cmd = "fstrim {}".format(self.conf['fs_mount_point'])
            utils.shcmd(cmd)

        if self.conf['filesystem'] == 'f2fs' and self.conf['f2fs_gc_after_workload'] is True:
            time.sleep(1)
            utils.drop_caches()
            utils.invoke_f2fs_gc(self.conf['fs_mount_point'], 1, -1)

        if self.conf['filesystem'] == 'ext4' and self.conf['dump_ext4_after_workload'] is True:
            utils.shcmd('sync')
            self.dumpe2fs()
            # self.dump_extents()

    def dumpe2fs(self):
        dumppath = os.path.join(self.conf['result_dir'], 'dumpe2fs.out')
        utils.shcmd("dumpe2fs {} > {}".format(
            self.conf['device_path'], dumppath))

    def dump_extents(self):
        dumppath = os.path.join(self.conf['result_dir'], 'extents.json')
        extent_path = dumppath + '.table'

        extents_list = get_extents_of_dir(dirpath=self.conf['fs_mount_point'],
                dev_path=self.conf['device_path'])
        d = {'extents': extents_list}
        utils.dump_json(d, dumppath)

        utils.table_to_file(extents_list, extent_path, width=0)

    def get_event_iterator(self):
        barriergen = BarrierGen(self.conf.ssd_ncq_depth())

        yield hostevent.ControlEvent(operation=OP_DISABLE_RECORDER)

        # mkfs events
        for event in self.prepfs_events():
            yield event

        # target workload event
        for event in self.target_workload_events():
            yield event

        # may send gc trigger
        for event in self.gc_event():
            yield event

        for req in barriergen.barrier_events():
            yield req
        yield hostevent.ControlEvent(operation=OP_REC_BW)

    def prepfs_events(self):
        prepfs_line_iter = hostevent.FileLineIterator(
            self.conf.get_ftlsim_events_output_path_mkfs())
        event_prepfs_iter = hostevent.EventIterator(self.conf, prepfs_line_iter)

        for event in event_prepfs_iter:
            yield event

    def target_workload_events(self):
        # special event indicates the start of workload
        barriergen = BarrierGen(self.conf.ssd_ncq_depth())
        yield hostevent.ControlEvent(operation=OP_ENABLE_RECORDER)
        for req in barriergen.barrier_events():
            yield req
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        workload_line_iter = hostevent.FileLineIterator(
            self.conf.get_ftlsim_events_output_path())
        event_workload_iter = hostevent.EventIterator(self.conf, workload_line_iter)

        for event in event_workload_iter:
            yield event

        for req in barriergen.barrier_events():
            yield req
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_end')

    def gc_event(self):
        barriergen = BarrierGen(self.conf.ssd_ncq_depth())
        if self.conf['do_gc_after_workload'] is True:
            for req in barriergen.barrier_events():
                yield req
            yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                    arg1='gc_start_timestamp')

            yield hostevent.ControlEvent(operation=OP_CLEAN)









