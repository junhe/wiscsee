from Makefile import *
import os

from workflow import run_workflow
from utilities.utils import get_expname
from utilities import utils
from config import MountOption as MOpt

class Experimenter(object):
    def __init__(self, para):
        if para.ftl == 'nkftl2':
            self.conf = ssdbox.nkftl2.Config()
        elif para.ftl == 'dftldes':
            self.conf = ssdbox.dftldes.Config()
        else:
            raise NotImplementedError()
        self.para = para
        self.conf['exp_parameters'] = self.para._asdict()

    def setup_environment(self):
        self.conf['device_path'] = self.para.device_path
        self.conf['dev_size_mb'] = self.para.lbabytes / MB
        self.conf["n_online_cpus"] = self.para.n_online_cpus

        self.conf['linux_ncq_depth'] = self.para.linux_ncq_depth

        set_vm_default()
        set_vm("dirty_bytes", self.para.dirty_bytes)

        self.conf['do_fstrim'] = False

        # filesystem
        self.conf['filesystem'] = self.para.filesystem

        if self.para.filesystem == 'ext4-nj':
            self.conf['filesystem'] = 'ext4'

    def setup_workload(self):
        raise NotImplementedError()

    def setup_fs(self):
        self.conf['mnt_opts'].update({
            "f2fs":   {
                        'discard': MOpt(opt_name = 'discard',
                                        value = 'discard',
                                        include_name = False),
                        # 'background_gc': MOpt(opt_name = 'background_gc',
                                            # value = 'off',
                                            # include_name = True)
                                        },
            "ext4":   {
                        'discard': MOpt(opt_name = "discard",
                                         value = "discard",
                                         include_name = False),
                        'data': MOpt(opt_name = "data",
                                        value = self.para.ext4datamode,
                                        include_name = True) },
            "btrfs":  {
                        "discard": MOpt(opt_name = "discard",
                                         value = "discard",
                                         include_name = False),
                                         "ssd": MOpt(opt_name = 'ssd',
                                             value = 'ssd',
                                     include_name = False),
                        "autodefrag": MOpt(opt_name = 'autodefrag',
                                            value = 'autodefrag',
                                            include_name = False) },
            "xfs":    {
                'discard': MOpt(opt_name = 'discard',
                                        value = 'discard',
                                        include_name = False)
                },
            }
            )

        if self.para.ext4hasjournal is True:
            utils.enable_ext4_journal(self.conf)
        else:
            utils.disable_ext4_journal(self.conf)

        if self.para.filesystem == 'ext4-nj':
            utils.disable_ext4_journal(self.conf)

        self.conf['f2fs_gc_after_workload'] = self.para.f2fs_gc_after_workload

    def setup_flash(self):
        self.conf['SSDFramework']['ncq_depth'] = self.para.ssd_ncq_depth

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = self.para.n_pages_per_block
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 16

        self.conf['do_not_check_gc_setting'] = True
        self.conf.GC_high_threshold_ratio = 0.90
        self.conf.GC_low_threshold_ratio = 0.0

    def setup_ftl(self):
        self.conf['enable_blktrace'] = self.para.enable_blktrace
        self.conf['enable_simulation'] = self.para.enable_simulation
        self.conf['stripe_size'] = self.para.stripe_size
        self.conf['segment_bytes'] = self.para.segment_bytes

        if self.para.ftl == 'dftldes':
            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['ftl_type'] = 'dftldes'
        elif self.para.ftl == 'nkftl2':
            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['ftl_type'] = 'nkftl2'

            self.conf['nkftl']['n_blocks_in_data_group'] = \
                self.para.segment_bytes / self.conf.block_bytes
            self.conf['nkftl']['max_blocks_in_log_group'] = \
                self.conf['nkftl']['n_blocks_in_data_group'] * 100
            print 'N:', self.conf['nkftl']['n_blocks_in_data_group']
            print 'K:', self.conf['nkftl']['max_blocks_in_log_group']
            self.conf['nkftl']['max_ratio_of_log_blocks'] = self.para.max_log_blocks_ratio
        else:
            raise NotImplementedError()

        logicsize_mb = self.conf['dev_size_mb']
        self.conf.cache_mapped_data_bytes = self.para.cache_mapped_data_bytes
        self.conf.set_flash_num_blocks_by_bytes(
                int(logicsize_mb * 2**20 * self.para.over_provisioning))

    def check_config(self):
        if self.conf['ftl_type'] == 'dftldes':
            assert isinstance(self.conf, ssdbox.dftldes.Config)
            assert self.conf['simulator_class'] == 'SimulatorDESNew'
        elif self.conf['ftl_type'] == 'nkftl2':
            assert isinstance(self.conf, ssdbox.nkftl2.Config)
            assert self.conf['simulator_class'] == 'SimulatorDESNew'
        else:
            RuntimeError("ftl type may not be supported here")

    def before_running(self):
        pass

    def run(self):
        # dict_for_name = {k:v for k,v in self.para._asdict() if k in ('
        set_exp_metadata(self.conf, save_data = True,
                expname = self.para.expname,
                subexpname = 'subexp-' + str(hash(chain_items_as_filename(self.para))))
        runtime_update(self.conf)

        self.check_config()

        run_workflow(self.conf)

    def main(self):
        self.setup_environment()
        self.setup_fs()
        self.setup_workload()
        self.setup_flash()
        self.setup_ftl()
        self.before_running()
        self.run()


def sqlbench():
    class Experimenter(object):
        def __init__(self, para):
            self.conf = ssdbox.dftldes.Config()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = self.para.device_path
            self.conf['dev_size_mb'] = self.para.lbabytes / MB
            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['linux_ncq_depth'] = self.para.linux_ncq_depth

            set_vm_default()
            set_vm("dirty_bytes", self.para.dirty_bytes)

        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {}
            self.conf['workload_conf_key'] = 'workload_config'
            self.conf['sqlbench'] = {'bench_to_run':self.para.bench_to_run}

            self.conf['f2fs_gc_after_workload'] = self.para.f2fs_gc_after_workload

        def setup_fs(self):
            self.conf['mnt_opts'].update({
                "f2fs":   {
                            'discard': MOpt(opt_name = 'discard',
                                            value = 'discard',
                                            include_name = False),
                            'background_gc': MOpt(opt_name = 'background_gc',
                                                value = 'off',
                                                include_name = True)
                                            },
                "ext4":   { 'discard': MOpt(opt_name = "discard",
                                             value = "discard",
                                             include_name = False),
                            'data': MOpt(opt_name = "data",
                                            value = self.para.ext4datamode,
                                            include_name = True) },
                }
                )

            if self.para.ext4hasjournal is True:
                utils.enable_ext4_journal(self.conf)
            else:
                utils.disable_ext4_journal(self.conf)

        def setup_flash(self):
            self.conf['SSDFramework']['ncq_depth'] = 8

            self.conf['flash_config']['page_size'] = 2048
            self.conf['flash_config']['n_pages_per_block'] = 64
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 8

            self.conf['do_not_check_gc_setting'] = True
            self.conf.GC_high_threshold_ratio = 0.96
            self.conf.GC_low_threshold_ratio = 0

        def setup_ftl(self):
            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = False
            self.conf['stripe_size'] = self.para.stripe_size

            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['ftl_type'] = 'dftldes'

            logicsize_mb = self.conf['dev_size_mb']
            self.conf.cache_mapped_data_bytes = self.para.cache_mapped_data_bytes
            self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.08))

        def run(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            run_workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_fs()
            self.setup_workload()
            self.setup_flash()
            self.setup_ftl()
            self.run()

    def run_exp():
        Parameters = collections.namedtuple("Parameters",
            "workload_class, filesystem, expname, dirty_bytes, device_path, "\
            "stripe_size, linux_ncq_depth, bench_to_run, "\
            "cache_mapped_data_bytes, lbabytes, "\
            "f2fs_gc_after_workload, ext4datamode, ext4hasjournal"
            )

        expname = get_expname()
        lbabytes = 1024*MB
        para_dict = {
                'workload_class'   : [
                    'Sqlbench',
                    ],
                'bench_to_run'   : [
                    'test-select',
                    'test-ATIS',
                    # 'test-create', # large data
                    'test-transactions',
                    'test-alter-table',
                    'test-connect',
                    # 'test-insert', # large data
                    'test-wisconsin',
                    ],
                'device_path'    : ['/dev/sdc1'],
                'filesystem'     : ['ext4'],
                'ext4datamode'   : ['ordered'],
                'ext4hasjournal' : [True],
                'expname'        : [expname],
                'dirty_bytes'    : [128*MB],
                'linux_ncq_depth': [31],
                'stripe_size'    : [1],
                'cache_mapped_data_bytes' :[lbabytes],
                'lbabytes'       : [lbabytes],

                'f2fs_gc_after_workload': [True],
                }
        parameter_combs = ParameterCombinations(para_dict)

        for para in parameter_combs:
            obj = Experimenter( Parameters(**para) )
            obj.main()

    run_exp()


def bench():
    class Experimenter(object):
        def __init__(self, para):
            self.conf = ssdbox.dftldes.Config()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = self.para.device_path
            self.conf['dev_size_mb'] = self.para.lbabytes / MB
            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['linux_ncq_depth'] = self.para.linux_ncq_depth

            set_vm_default()
            set_vm("dirty_bytes", self.para.dirty_bytes)

        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {}
            self.conf['workload_conf_key'] = 'workload_config'

            self.conf['f2fs_gc_after_workload'] = self.para.f2fs_gc_after_workload

        def setup_fs(self):
            self.conf['mnt_opts'].update({
                "f2fs":   {
                            # 'discard': MOpt(opt_name = 'discard',
                                            # value = 'discard',
                                            # include_name = False),
                            'background_gc': MOpt(opt_name = 'background_gc',
                                                value = 'off',
                                                include_name = True)
                                            },
                "ext4":   {
                            # 'discard': MOpt(opt_name = "discard",
                                             # value = "discard",
                                             # include_name = False),
                            'data': MOpt(opt_name = "data",
                                            value = self.para.ext4datamode,
                                            include_name = True) },
                }
                )

            if self.para.ext4hasjournal is True:
                utils.enable_ext4_journal(self.conf)
            else:
                utils.disable_ext4_journal(self.conf)

        def setup_flash(self):
            self.conf['SSDFramework']['ncq_depth'] = 8

            self.conf['flash_config']['page_size'] = 2048
            self.conf['flash_config']['n_pages_per_block'] = 64
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 8

            self.conf['do_not_check_gc_setting'] = True
            self.conf.GC_high_threshold_ratio = 0.96
            self.conf.GC_low_threshold_ratio = 0

        def setup_ftl(self):
            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = False
            self.conf['stripe_size'] = self.para.stripe_size

            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['ftl_type'] = 'dftldes'

            logicsize_mb = self.conf['dev_size_mb']
            self.conf.cache_mapped_data_bytes = self.para.cache_mapped_data_bytes
            self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.08))

        def run(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            run_workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_fs()
            self.setup_workload()
            self.setup_flash()
            self.setup_ftl()
            self.run()

    def run_exp():
        Parameters = collections.namedtuple("Parameters",
            "workload_class, filesystem, expname, dirty_bytes, device_path, "\
            "stripe_size, linux_ncq_depth, "\
            "cache_mapped_data_bytes, lbabytes, "\
            "f2fs_gc_after_workload, ext4datamode, ext4hasjournal"
            )

        expname = get_expname()
        lbabytes = 8*GB
        para_dict = {
                'workload_class'   : [
                    'Tpcc',
                    ],
                'device_path'    : ['/dev/sdc1'],
                'filesystem'     : ['ext4'],
                'ext4datamode'   : ['ordered'],
                'ext4hasjournal' : [True],
                'expname'        : [expname],
                'dirty_bytes'    : [128*MB],
                'linux_ncq_depth': [31],
                'stripe_size'    : [1],
                'cache_mapped_data_bytes' :[lbabytes],
                'lbabytes'       : [lbabytes],

                'f2fs_gc_after_workload': [True],
                }
        parameter_combs = ParameterCombinations(para_dict)

        for para in parameter_combs:
            obj = Experimenter( Parameters(**para) )
            obj.main()

    run_exp()


def leveldbbench():
    class LocalExperimenter(Experimenter):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    'one_by_one': self.para.one_by_one,
                    'threads': self.para.leveldb_threads,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

    class ParaDict(object):
        def __init__(self):
            expname = get_expname()
            lbabytes = 1*GB
            para_dict = {
                    'ftl'            : ['nkftl2'],
                    'device_path'    : ['/dev/sdc1'],
                    # 'filesystem'     : ['f2fs', 'ext4', 'ext4-nj', 'btrfs', 'xfs'],
                    'filesystem'     : ['ext4'],
                    'ext4datamode'   : ['ordered'],
                    'ext4hasjournal' : [True],
                    'expname'        : [expname],
                    'dirty_bytes'    : [4*GB],
                    'linux_ncq_depth': [31],
                    'ssd_ncq_depth'  : [1],
                    'cache_mapped_data_bytes' :[lbabytes],
                    'lbabytes'       : [lbabytes],
                    'n_pages_per_block': [64],
                    'stripe_size'    : [64],
                    'enable_blktrace': [True],
                    'enable_simulation': [True],
                    'f2fs_gc_after_workload': [False],
                    'segment_bytes'  : [128*KB],
                    'max_log_blocks_ratio': [8],
                    'n_online_cpus'  : ['all'],
                    'over_provisioning': [8], # 1.28 is a good number

                    'workload_class' : [
                        'Leveldb'
                        # 'IterDirs'
                        ],
                    'benchconfs': [
                        # [{'benchmarks': 'fillseq',   'num': 3*2000000, 'max_key': None},
                         # {'benchmarks': 'overwrite', 'num': 3*2000000, 'max_key': 4*2000}],
                        [{'benchmarks': 'fillseq',   'num': 1000000, 'max_key': None},
                         {'benchmarks': 'overwrite', 'num': 1000000, 'max_key': 1000}],
                        ],
                    'leveldb_threads': [1],
                    'one_by_one'     : [False],
                    }
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def sqlitebench():
    class LocalExperimenter(Experimenter):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

    class ParaDict(object):
        def __init__(self):
            expname = get_expname()
            lbabytes = 1*GB
            para_dict = {
                    'ftl'            : ['nkftl2'],
                    'device_path'    : ['/dev/sdc1'],
                    # 'filesystem'     : ['f2fs', 'ext4', 'ext4-nj', 'btrfs', 'xfs'],
                    'filesystem'     : ['ext4'],
                    'ext4datamode'   : ['ordered'],
                    'ext4hasjournal' : [True],
                    'expname'        : [expname],
                    'dirty_bytes'    : [4*GB],
                    'linux_ncq_depth': [31],
                    'ssd_ncq_depth'  : [1],
                    'cache_mapped_data_bytes' :[lbabytes],
                    'lbabytes'       : [lbabytes],
                    'n_pages_per_block': [64],
                    'stripe_size'    : [64],
                    'enable_blktrace': [True],
                    'enable_simulation': [True],
                    'f2fs_gc_after_workload': [False],
                    'segment_bytes'  : [128*KB],
                    'max_log_blocks_ratio': [2],
                    'n_online_cpus'  : ['all'],

                    'workload_class' : [
                        'Sqlite'
                        ],
                    'benchconfs': [
                            [
                            {'pattern': 'random', 'n_insertions': 10000},
                            {'pattern': 'sequential', 'n_insertions': 10000},
                            ]
                        ],
                    }
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def newsqlbench():
    class LocalExperimenter(Experimenter):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {}
            self.conf['workload_conf_key'] = 'workload_config'
            self.conf['sqlbench'] = {'bench_to_run':self.para.bench_to_run}

    class ParaDict(object):
        def __init__(self):
            expname = get_expname()
            lbabytes = 1*GB
            para_dict = {
                    'ftl'            : ['nkftl2'],
                    'device_path'    : ['/dev/sdc1'],
                    # 'filesystem'     : ['f2fs', 'xfs', 'ext4', 'btrfs'],
                    'filesystem'     : ['ext4-nj'],
                    'ext4datamode'   : ['ordered'],
                    'ext4hasjournal' : [False],
                    'expname'        : [expname],
                    'dirty_bytes'    : [4*GB],
                    'linux_ncq_depth': [31],
                    'ssd_ncq_depth'  : [1],
                    'cache_mapped_data_bytes' :[lbabytes],
                    'lbabytes'       : [lbabytes],
                    'n_pages_per_block': [64],
                    'stripe_size'    : [64],
                    'enable_blktrace': [True],
                    'enable_simulation': [True],
                    'f2fs_gc_after_workload': [True],
                    'segment_bytes'  : [128*KB, 16*MB],
                    'max_log_blocks_ratio': [2],

                    'workload_class' : [
                        'Sqlbench'
                        ],
                    'bench_to_run': [ 'test-insert-rand' ],
                    }
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def filesnakebench():
    class LocalExperimenter(Experimenter):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

    class ParaDict(object):
        def __init__(self):
            expname = get_expname()
            lbabytes = 1*GB
            para_dict = {
                    'ftl'            : ['nkftl2'],
                    'device_path'    : ['/dev/sdc1'],
                    # 'filesystem'     : ['f2fs', 'ext4', 'ext4-nj', 'btrfs', 'xfs'],
                    'filesystem'     : ['ext4', 'f2fs'],
                    'ext4datamode'   : ['ordered'],
                    'ext4hasjournal' : [True],
                    'expname'        : [expname],
                    'dirty_bytes'    : [4*GB],
                    'linux_ncq_depth': [31],
                    'ssd_ncq_depth'  : [1],
                    'cache_mapped_data_bytes' :[lbabytes],
                    'lbabytes'       : [lbabytes],
                    'n_pages_per_block': [64],
                    'stripe_size'    : [64],
                    'enable_blktrace': [True],
                    'enable_simulation': [True],
                    'f2fs_gc_after_workload': [False],
                    'segment_bytes'  : [128*KB],
                    'max_log_blocks_ratio': [2],
                    'n_online_cpus'  : ['all'],
                    'over_provisioning': [4], # 1.28 is a good number

                    'workload_class' : [
                        'FileSnake'
                        ],
                    'benchconfs': [
                        {'zone_len': int(1.5*GB/(128*KB)),
                            'snake_len': 2048,
                            'file_size': 128*KB,
                            'write_pre_file': True
                            },
                        {'zone_len': int(1.5*GB/(128*KB)),
                            'snake_len': 2048,
                            'file_size': 128*KB,
                            'write_pre_file': False
                            }
                        ],
                    }
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()



def reproduce():
    class LocalExperimenter(Experimenter):
        def setup_workload(self):
            self.conf["workload_src"] = LBAGENERATOR

            self.conf["lba_workload_class"] = "BlktraceEvents"

            self.conf['lba_workload_configs']['mkfs_event_path'] = \
                    self.para.event_file_pair['mkfs_path']
            self.conf['lba_workload_configs']['ftlsim_event_path'] = \
                    self.para.event_file_pair['ftlsim_path']

        def before_running(self):
            original_config = self.para.event_file_pair['original_config']
            to_update = {k:v for k,v in original_config.items() \
                    if k in ('filesystem')}
            self.conf.update(to_update)

            try:
                self.conf['exp_parameters']['bench_to_run'] = \
                    original_config['exp_parameters']['bench_to_run']
            except KeyError:
                pass

    class ParaDict(object):
        def __init__(self):
            expname = get_expname()

            pair_list = EventFilePairs('/tmp/results/mytest')

            lbabytes = 1*GB
            para_dict = {
                    'ftl'            : ['nkftl2'],
                    'device_path'    : [None],
                    'filesystem'     : [None],
                    'ext4datamode'   : [None],
                    'ext4hasjournal' : [None],
                    'expname'        : [expname],
                    'dirty_bytes'    : [4*GB],
                    'linux_ncq_depth': [31],
                    'ssd_ncq_depth'  : [1],
                    'cache_mapped_data_bytes' :[lbabytes],
                    'lbabytes'       : [lbabytes],
                    'n_pages_per_block': [64],
                    'stripe_size'    : [64],
                    'enable_blktrace': [None],
                    'enable_simulation': [True],
                    'f2fs_gc_after_workload': [False],
                    'segment_bytes'  : [128*KB],
                    'max_log_blocks_ratio' : [2.0],

                    'event_file_pair': pair_list.get_pairs(),
                    }
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    class EventFilePairs(object):
        def __init__(self, dirpath):
            self.dirpath = dirpath

        def get_pairs(self):
            """
            iterate directories and return pairs of mkfs and ftlsim file paths
            """
            pairs = []
            for root, dirs, files in os.walk(self.dirpath, topdown=False):
                for name in files:
                    if name == 'blkparse-events-for-ftlsim-mkfs.txt':
                        mkfs_path = os.path.join(root, 'blkparse-events-for-ftlsim-mkfs.txt')
                        ftlsim_path = os.path.join(root, 'blkparse-events-for-ftlsim.txt')

                        confjson = self._get_confjson(root)
                        d = {'mkfs_path': mkfs_path,
                             'ftlsim_path': ftlsim_path,
                             'original_config': confjson,
                             }

                        if self._keep_subexp(confjson) is True:
                            pairs.append(d)

            return pairs

        def _keep_subexp(self, confjson):
            """
            Return; true/false
            """
            return True
            if all( [ confjson['filesystem'] in ('ext4', 'f2fs', 'xfs', 'btrfs'),
                      confjson['segment_bytes'] == 128*KB,
                      confjson['exp_parameters']['bench_to_run'] == 'test-insert-rand' ]):
                return True
            else:
                return False

        def _get_confjson(self, subexp_path):
            confpath = os.path.join(subexp_path, 'config.json')
            confjson = load_json(confpath)

            return confjson


    def main():
        for para in ParaDict():
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()





def main(cmd_args):
    if cmd_args.git == True:
        shcmd("sudo -u jun git commit -am 'commit by Makefile: {commitmsg}'"\
            .format(commitmsg=cmd_args.commitmsg \
            if cmd_args.commitmsg != None else ''), ignore_error=True)
        shcmd("sudo -u jun git pull")
        shcmd("sudo -u jun git push")


def _main():
    parser = argparse.ArgumentParser(
        description="This file hold command stream." \
        'Example: python Makefile.py doexp1 '
        )
    parser.add_argument('-t', '--target', action='store')
    parser.add_argument('-c', '--commitmsg', action='store')
    parser.add_argument('-g', '--git',  action='store_true',
        help='snapshot the code by git')
    args = parser.parse_args()

    if args.target == None:
        main(args)
    else:
        # WARNING! Using argument will make it less reproducible
        # because you have to remember what argument you used!
        targets = args.target.split(';')
        for target in targets:
            eval(target)
            # profile.run(target)

if __name__ == '__main__':
    _main()





