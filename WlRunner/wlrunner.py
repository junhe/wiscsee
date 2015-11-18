import time

import blocktrace
import config
import filesystem
import fshelper
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

        if self.conf['device_type'] == 'loop':
            # create loop dev object, it is not physically created yet
            self.loopdev = filesystem.LoopDevice(
                dev_path = self.conf['device_path'],
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
            "workload_conf = {wlconf})".format(
                wlclass = self.conf["age_workload_class"],
                wlconf = self.conf["aging_config"]
                ) #format
            ) #eval

        # create workload object
        self.workload = eval("workload.{wlclass}(confobj = self.conf, " \
            "workload_conf = {wlconf})".format(
                wlclass = self.conf["workload_class"],
                wlconf = self.conf["Synthetic"]
                ) #format
            ) #eval

        # create Ftrace manager
        # self.ftrace = ftrace.Ftrace()

    def run(self):
        try:
            # Prepare file systems
            if self.conf['device_type'] == 'loop':
                self.loopdev.create()
            elif self.conf['device_type'] == 'real':
                # umount file system if it is mounted
                if fshelper.isMounted(self.conf['fs_mount_point']):
                    utils.shcmd(
                        "sudo umount {}".format(self.conf['fs_mount_point']))

            # strat blktrace
            # This is only for making and mounting file system, because we
            # want to separate them with workloads.
            self.blktracer_mkfs.start_tracing_and_collecting()
            time.sleep(0.5)
            while self.blktracer_mkfs.proc == None:
                print 'Waiting for blktrace to start.....'
                time.sleep(0.5)

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

            # ftr = ftrace.Ftrace()
            # ftr.clean_trace()
            # ftr.set_tracer('function_graph')
            # ftr.set_tracer('function')
            # ftr.write_file('options/func_stack_trace', '1')
            # ftr.set_filter('*ext4*')
            # ftr.set_filter('f2fs_trace_ios')
            # ftr.add_filter('write_checkpoint')
            # ftr.add_filter('do_checkpoint')
            # ftr.set_filter(':mod:f2fs')
            # ftr.start_tracing()
            # time.sleep(2)
            # ftr.write_marker('JUNJun marked this beginning.')

            print 'Running workload ....'
            self.workload.run()

            utils.shcmd('sync')

            # ftr.write_marker('Jun marked this end.')
            # ftr.stop_tracing()

        except Exception:
            raise
        else:
            time.sleep(1)
            self.blktracer.blkparse_file_to_ftlsim_input_file()
            return self.get_event_iterator()
        finally:
            # always try to clean up the blktrace processes
            self.blktracer.stop_tracing_and_collecting()

    def get_event_iterator(self):
        yield "disable_recorder 0 0"

        mkfs_iter = FileLineIterator(
            self.conf.get_ftlsim_events_output_path_mkfs())

        for event in mkfs_iter:
            yield event

        # special event indicates the start of workload
        yield "enable_recorder 0 0"
        yield "workloadstart 0 0"

        workload_iter = FileLineIterator(
            self.conf.get_ftlsim_events_output_path())

        for event in workload_iter:
            yield event


