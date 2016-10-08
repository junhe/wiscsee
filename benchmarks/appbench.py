from Makefile import *
import csv
import os
import glob

from utilities import utils
from experimenter import *
from expconfs import ParameterPool
import filesim

import prepare4pyreuse
from pyreuse.sysutils.straceParser import parse_and_write_dirty_table


class StatsMixin(object):
    def write_stats(self):
        stats_path = os.path.join(self.conf['result_dir'], 'stats.json')
        disk_used_bytes = utils.get_dir_size(self.conf['fs_mount_point'])

        written_bytes = self.get_traffic_size()

        d = {'disk_used_bytes': disk_used_bytes,
             'written_bytes': written_bytes
            }
        utils.dump_json(d, stats_path)
        print 'disk_used_bytes', disk_used_bytes / GB
        print 'written_bytes', written_bytes / GB

    def get_traffic_size(self):
        filepath = os.path.join(self.conf['result_dir'], 'blkparse-events-for-ftlsim.txt')
        with open(filepath, 'rb') as f:
            reader = csv.reader(f, delimiter=' ')
            total = 0
            for row in reader:
                op = row[1]
                size = int(row[3])
                if op == 'write':
                    total += size
        return total

class ParaDictIterMixin(object):
    def iterate_blocksize_segsize_fs(self):
        para = self.parameter_combs[0]
        lbabytes = para['lbabytes']
        updatedicts = [
            # {'segment_bytes': 2*MB, 'n_pages_per_block': 128*KB/(2*KB)},
            # {'segment_bytes': 16*MB,        'n_pages_per_block': 128*KB/(2*KB)},
            # {'segment_bytes': lbabytes * 2, 'n_pages_per_block': 128*KB/(2*KB)},

            # {'segment_bytes': 16*MB,        'n_pages_per_block': 1*MB/(2*KB)},
            # {'segment_bytes': 128*MB,        'n_pages_per_block': 1*MB/(2*KB)},
            {'segment_bytes': lbabytes * 2, 'n_pages_per_block': 1*MB/(2*KB)},
            ]
        new_update_dics = []
        for d in updatedicts:
            for fs in ['ext4', 'f2fs', 'xfs', 'btrfs']:
            # for fs in ['btrfs', 'xfs']:
                new_d = copy.copy(d)
                new_d['filesystem'] = fs

                new_update_dics.append(new_d)

        for update_dict in new_update_dics:
            tmp_para = copy.deepcopy(para)
            tmp_para.update(update_dict)
            yield tmp_para

    def iterate_blocksize_for_alignment(self):
        local_paras = []
        for parameters in self.parameter_combs:
            for block_size in self.block_sizes:
                para = copy.deepcopy(parameters)
                para['n_pages_per_block'] = block_size / (2*KB)
                para['stripe_size'] = para['n_pages_per_block']
                para['segment_bytes'] = block_size

                local_paras.append(para)

        for para in local_paras:
            yield para


def leveldbbench_for_locality():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    'one_by_one': self.para.one_by_one,
                    'threads': self.para.leveldb_threads,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'ftl'            : ['dftldes'],
                    'filesystem'     : ['f2fs', 'xfs', 'ext4', 'btrfs'],
                    'cache_mapped_data_bytes' :[int(0.1*lbabytes)
                                                ],
                    'workload_class' : ['Leveldb'],
                    'benchconfs': [
                            [{'benchmarks': 'overwrite',  'num': 6*1000000,
                                'max_key': 6*1000000, 'max_log': -1}],
                        ],
                    'leveldb_threads': [1],
                    'one_by_one'     : [False],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()

def leveldbbench_for_wearleveling():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    'one_by_one': self.para.one_by_one,
                    'threads': self.para.leveldb_threads,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'ftl'              : ['dftldes'],
                    'over_provisioning': [1.28], # 1.28 is a good number
                    'gc_high_ratio'    : [0.9],
                    'gc_low_ratio'     : [0.8],
                    'not_check_gc_setting': [False],
                    'filesystem'       : ['ext4'],
                    'cache_mapped_data_bytes' :[int(1*lbabytes) ],
                    'workload_class' : ['Leveldb'],
                    'benchconfs': [
                            [{'benchmarks': 'overwrite',  'num': 6*1000000,
                                'max_key': 6*1000000, 'max_log': -1}],
                        ],
                    'leveldb_threads': [1],
                    'one_by_one'     : [False],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()




def leveldbbench():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    'one_by_one': self.para.one_by_one,
                    'threads': self.para.leveldb_threads,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'ftl': ['dftldes'],
                    'workload_class' : [
                        'Leveldb'
                        ],
                    'benchconfs': [
                            [{'benchmarks': 'overwrite',  'num': 3*1000000, 'max_key': 3*1000000, 'max_log': -1}],
                        ],
                    'leveldb_threads': [1],
                    'one_by_one'     : [False],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            # return iter(self.parameter_combs)
            return iter(self.iterate_blocksize_segsize_fs())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def leveldbbench_for_alignment():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    'one_by_one': self.para.one_by_one,
                    'threads': self.para.leveldb_threads,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 8*GB
            para_dict = get_shared_para_dict(expname, lbabytes)

            self.block_sizes = [1*MB, 128*KB]
            para_dict.update( {
                    'ftl'          : ['nkftl2'],
                    'enable_simulation': [True],
                    'filesystem'        : ['f2fs'],
                    'n_pages_per_block' : [None],
                    'stripe_size'       : [None],
                    'segment_bytes'     : [None],
                    'max_log_blocks_ratio': [100],
                    'over_provisioning' : [32], # 1.28 is a good number

                    'workload_class' : [
                        'Leveldb'
                        ],
                    'benchconfs': [
                            [{'benchmarks': 'overwrite',
                                'num': 60*1000000,
                                'max_key': 60*1000000,
                                'max_log': -1}],
                        ],
                    'leveldb_threads': [1],
                    'one_by_one'     : [False],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.iterate_blocksize_for_alignment())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def sqlitebench():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'workload_class' : [
                        'Sqlite'
                        ],
                    'benchconfs': [
                            [
                            {'pattern': 'random', 'n_insertions': 120000,
                                'commit_period': 10, 'max_key': 120000},
                            ]
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)
            # return iter(self.iterate_blocksize_segsize_fs())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def sqlitebench_for_locality():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'ftl'            : ['dftldes'],
                    'filesystem'     : ['f2fs', 'xfs', 'ext4', 'btrfs'],
                    'cache_mapped_data_bytes' :[
                        int(0.1 * lbabytes),
                        int(0.05 * lbabytes),
                        int(0.01 * lbabytes),
                        ],
                    'workload_class' : ['Sqlite'],
                    'benchconfs': [
                            [
                            {'pattern': 'random', 'n_insertions': 240000,
                                'commit_period': 10, 'max_key': 120000},
                            ]
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()



def sqlitebench_for_alignment():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'benchconfs': self.para.benchconfs,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)

            self.block_sizes = [128*KB, 1*MB]
            para_dict.update( {
                    'ftl'               : ['nkftl2'],
                    'filesystem'        : ['ext4', 'f2fs'],
                    'n_pages_per_block' : [None],
                    'stripe_size'       : [None],
                    'segment_bytes'     : [None],
                    'max_log_blocks_ratio': [100],
                    'over_provisioning' : [16], # 1.28 is a good number

                    'workload_class'    : [
                        'Sqlite'
                        ],
                    'benchconfs'        : [
                            [
                            {'pattern': 'random', 'n_insertions': 120000,
                                'commit_period': 10, 'max_key': 120000},
                            ]
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.iterate_blocksize_for_alignment())

    def main():
        for para in ParaDict():
            print para
            # continue
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def varmailbench():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'workload_class' : [
                        'Varmail'
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            # return iter(self.parameter_combs)
            return iter(self.iterate_blocksize_segsize_fs())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()



def varmailbench_for_locality():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'ftl'            : ['dftldes'],
                    'filesystem'     : ['f2fs', 'xfs', 'ext4', 'btrfs'],
                    'cache_mapped_data_bytes' :[
                        int(0.1 * lbabytes),
                        int(0.5 * lbabytes),
                        int(1 * lbabytes)
                        # int(0.05 * lbabytes),
                        # int(0.1 * lbabytes)
                        ],

                    'workload_class' : [
                        'Varmail'
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def varmailbench_for_alignment():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)

            self.block_sizes = [128*KB, 1*MB]
            para_dict.update( {
                    'ftl'               : ['nkftl2'],
                    'filesystem'        : ['f2fs'],
                    'f2fs_ipu_policy'   : [F2FS_IPU_FSYNC],
                    'f2fs_min_fsync_blocks': [0],

                    'n_pages_per_block' : [None],
                    'stripe_size'       : [None],
                    'segment_bytes'     : [None],

                    'max_log_blocks_ratio': [100],
                    'over_provisioning' : [16], # 1.28 is a good number

                    'workload_class' : [
                        'Varmail'
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.iterate_blocksize_for_alignment())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def appmixbench():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'appconfs': self.para.appconfs,
                    'run_seconds': self.para.run_seconds,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

            self.conf['age_workload_class'] = self.para.age_workload_class
            self.conf['aging_workload_config'] = {
                    'appconfs': self.para.aging_appconfs,
                    'run_seconds': None,
                    }
            self.conf['aging_config_key'] = 'aging_workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'workload_class' : [ 'AppMix' ],
                    'run_seconds'    : [None],

                    'age_workload_class': ['AppMix'],
                    'aging_appconfs': [
                            [
                                {'name': 'Sqlite',
                                 'pattern': 'random',
                                 'n_insertions': 12000,
                                 'commit_period': 10,
                                 'max_key': 20,
                                 'do_strace': False
                                },
                            ]
                        ],

                    'appconfs': [
                            [ # list of app you want to run

                            # ---- TEMPLATE ------
                            # {'name' : 'LevelDB',
                             # 'benchmarks': 'overwrite',
                             # 'num': 1*1000000,
                             # 'max_key': 1*100000,
                             # 'max_log': -1},

                            {'name': 'Sqlite',
                             'pattern': 'random',
                             'n_insertions': 12000,
                             'commit_period': 10,
                             'max_key': 20,
                             'do_strace': False
                            },

                            # {'name': 'Varmail',
                            #  'nfiles': 8000
                             # 'seconds': 2},
                             # -------------

                            # {
                                # "name": "Varmail",
                                # "nfiles": 200,
                                # "seconds": 600,
                            # },
                            # {
                                # "name": "Varmail",
                                # "nfiles": 50,
                                # "seconds": 600,
                            # }
                            ]
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            # return iter(self.parameter_combs)
            return iter(self.iterate_blocksize_segsize_fs())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def appmixbench_for_rw():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'appconfs': self.para.appconfs,
                    'run_seconds': self.para.run_seconds,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

            self.conf['age_workload_class'] = self.para.age_workload_class
            self.conf['aging_workload_config'] = {
                    'appconfs': self.para.aging_appconfs,
                    'run_seconds': None,
                    }
            self.conf['aging_config_key'] = 'aging_workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __iter__(self):
            expname = utils.get_expname()
            para_pool = ParameterPool(
                    expname = expname,
                    testname = [
                        'tmptest_na_na_na'

                        # 'rocksdb_reqscale_r_seq',
                        # 'rocksdb_reqscale_r_rand',
                        # 'rocksdb_reqscale_r_mix',
                        # 'rocksdb_reqscale_w_seq',
                        # 'rocksdb_reqscale_w_rand',
                        # 'rocksdb_reqscale_w_mix'

                        # 'leveldb_reqscale_r_seq',
                        # 'leveldb_reqscale_r_rand',
                        # 'leveldb_reqscale_r_mix',
                        # 'leveldb_reqscale_w_seq',
                        # 'leveldb_reqscale_w_rand',
                        # 'leveldb_reqscale_w_mix'

                        # 'sqliteWAL_reqscale_r_seq',
                        # 'sqliteWAL_reqscale_r_rand',
                        # 'sqliteWAL_reqscale_r_mix',
                        # 'sqliteWAL_reqscale_w_seq',
                        # 'sqliteWAL_reqscale_w_rand',
                        # 'sqliteWAL_reqscale_w_mix'

                        # 'sqliteRB_reqscale_r_seq',
                        # 'sqliteRB_reqscale_r_rand',
                        # 'sqliteRB_reqscale_r_mix',
                        # 'sqliteRB_reqscale_w_seq',
                        # 'sqliteRB_reqscale_w_rand',
                        # 'sqliteRB_reqscale_w_mix'

                        'varmail_reqscale_r_small',
                        'varmail_reqscale_r_large',
                        'varmail_reqscale_r_mix',
                        'varmail_reqscale_w_small',
                        'varmail_reqscale_w_large',
                        'varmail_reqscale_w_mix'
                        ],
                    filesystem = ['ext4', 'f2fs', 'xfs']
                    # filesystem = ['ext4']
                    )

            return iter(para_pool)

    def main():
        for para in ParaDict():
            print '------------------------------------------'
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def appmixbench_for_scaling():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'appconfs': self.para.appconfs,
                    'run_seconds': self.para.run_seconds,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

            strace_files = glob.glob('/tmp/*strace.out')
            for filepath in strace_files:
                os.remove(filepath)

        def after_running(self):
            result_dir = self.conf['result_dir']
            strace_files = glob.glob('/tmp/*strace.out')
            print 'strace files', strace_files
            for filepath in strace_files:
                print 'parsing', filepath
                filename = os.path.basename(filepath)
                dirty_table_path = os.path.join(result_dir,
                        filename + '.dirty_table')

                parse_and_write_dirty_table(filepath, dirty_table_path)
                os.remove(filepath)

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 16*GB
            para_dict = get_shared_para_dict(expname, lbabytes)

            leveldb_inst = { 'name' : 'LevelDB',
                             'benchmarks': 'overwrite',
                             'num': 10*MILLION,
                             'max_key': 1*MILLION,
                             'max_log': -1,
                             'do_strace': False
                            }

            sqlite_inst = {'name': 'Sqlite',
                            'pattern': 'random',
                            'n_insertions': 120000,
                            'max_key': 120000,
                            'commit_period': 10,
                            'do_strace': False,
                           }
            varmail_inst = {
                             "name": "Varmail",
                             "nfiles": 8000,
                             "seconds": 360000,
                             "num_ops": 1*MILLION,
                             'do_strace': False,
                            }


            para_dict.update( {
                    'ftl' : ['ftlcounter'],
                    'workload_class' : [ 'AppMix' ],
                    'dump_ext4_after_workload': [False],
                    'only_get_traffic': [True],
                    'run_seconds'    : [None],

                    'enable_blktrace': [False],
                    'enable_simulation': [False],

                    'filesystem'     : ['ext4', 'f2fs', 'xfs'],

                    'do_ncq_depth_time_line': [True],
                    'fs_discard': [True],
                    'appconfs': [
                            [ leveldb_inst ] * 1,
                            [ leveldb_inst ] * 2,
                            [ leveldb_inst ] * 4,
                            [ leveldb_inst ] * 8,
                            [ leveldb_inst ] * 16,
                            [ leveldb_inst ] * 32,
                            [ leveldb_inst ] * 64,

                            [ sqlite_inst ] * 1,
                            [ sqlite_inst ] * 2,
                            [ sqlite_inst ] * 4,
                            [ sqlite_inst ] * 8,
                            [ sqlite_inst ] * 16,
                            [ sqlite_inst ] * 32,
                            [ sqlite_inst ] * 64,

                            [varmail_inst] * 1,
                            [varmail_inst] * 2,
                            [varmail_inst] * 4,
                            [varmail_inst] * 8,
                            [varmail_inst] * 16,
                            [varmail_inst] * 32,
                            [varmail_inst] * 64,

                            # [
                                # {
                                 # "name": "F2FStest",
                                # },
                            # ],

                            # [
                                # {
                                 # "name": "Varmail",
                                 # "nfiles": 8000,
                                 # "seconds": 360000,
                                 # "num_ops": 60*MILLION,
                                # },
                            # ],
                            # [
                               # {'name': 'Sqlite',
                                # 'pattern': 'random',
                                # 'n_insertions': 12000000,
                                # 'max_key': 120000,
                                # 'commit_period': 10,
                               # },
                            # ],
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)
            # return iter(self.iterate_blocksize_segsize_fs())

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()




def appmixbench_for_wearleveling():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'appconfs': self.para.appconfs,
                    'run_seconds': self.para.run_seconds,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)
            para_dict.update( {
                    'ftl'              : ['dftldes'],
                    'enable_simulation': [True],
                    'over_provisioning': [1.5], # 1.28 is a good number
                    'gc_high_ratio'    : [0.9],
                    'gc_low_ratio'     : [0.8],
                    'not_check_gc_setting': [False],
                    'cache_mapped_data_bytes' :[int(0.1*lbabytes)],
                    'segment_bytes'    : [lbabytes],
                    'snapshot_interval': [1*SEC],
                    'write_gc_log'     : [False],

                    'workload_class' : [ 'AppMix' ],
                    'run_seconds'    : [None],
                    'appconfs': [
                            [ # list of app you want to run

                            # ---- TEMPLATE ------
                            # {'name' : 'LevelDB',
                             # 'benchmarks': 'overwrite',
                             # 'num': 1*1000000,
                             # 'max_key': 1*100000,
                             # 'max_log': -1},

                            # {'name': 'Sqlite',
                             # 'pattern': 'random',
                             # 'n_insertions': 12000,
                             # 'commit_period': 10,
                             # 'max_key': 20
                             #},

                            # {'name': 'Varmail',
                            #  'nfiles': 8000
                             # 'seconds': 2},
                             # -------------

                            # for wear-leveling
                            # {'name' : 'LevelDB',
                             # 'benchmarks': 'overwrite',
                             # 'num': 6*1000000,
                             # 'max_key': 6*1000000,
                             # 'max_log': -1},

                            {'name': 'Sqlite',
                             'pattern': 'random',
                             'n_insertions': 12000,
                             'commit_period': 10,
                             'max_key': 12000,
                            },
                            ]
                        ],
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            # return iter(self.parameter_combs)
            # return iter(self.iterate_blocksize_segsize_fs())
            return iter([list(self.iterate_blocksize_segsize_fs())[0]])

    def main():
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()


def appmixbench_for_bw():
    class LocalExperimenter(Experimenter, StatsMixin):
        def setup_workload(self):
            self.conf['workload_class'] = self.para.workload_class
            self.conf['workload_config'] = {
                    'appconfs': self.para.appconfs,
                    'run_seconds': self.para.run_seconds,
                    }
            self.conf['workload_conf_key'] = 'workload_config'

        def after_running(self):
            self.write_stats()

    class ParaDict(ParaDictIterMixin):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 1*GB
            para_dict = get_shared_para_dict(expname, lbabytes)

            leveldb_insts = [
                    {'name' : 'LevelDB',
                     'benchmarks': 'overwrite',
                     'num': int(0.01*MILLION),
                     'max_key': 6*1000000,
                     'max_log': -1},
                    {'name' : 'LevelDB',
                     'benchmarks': 'fillseq',
                     'num': int(0.01*MILLION),
                     'max_key': 6*1000000,
                     'max_log': -1}
                ]

            leveldb_insts_long = [
                {
                    "max_key": 2000000,
                    "benchmarks": "fillseq",
                    "num": 1500000,
                    "name": "LevelDB",
                    "max_log": -1,
                    "do_strace": False,
                },
                {
                    "max_key": 300000,
                    "benchmarks": "overwrite",
                    "num": 4500000,
                    "name": "LevelDB",
                    "max_log": -1,
                    "do_strace": False,
                } ]


            sqlite_insts = [
                    {'name': 'Sqlite',
                     'pattern': 'random',
                     'n_insertions': 1200,
                     'commit_period': 10,
                     'max_key': 120000,
                    },
                    {'name': 'Sqlite',
                     'pattern': 'sequential',
                     'n_insertions': 1200,
                     'commit_period': 10,
                     'max_key': 120000,
                    }
                ]

            sqlite_insts_long = [
                    {
                        "commit_period": 50,
                        "pattern": "sequential",
                        "n_insertions": 250000,
                        "name": "Sqlite",
                        "max_key": 120000,
                        "do_strace": False,
                    },
                    {
                        "commit_period": 50,
                        "pattern": "random",
                        "n_insertions": 250000,
                        "name": "Sqlite",
                        "max_key": 120000,
                        "do_strace": False,
                    }]



            varmail_insts = [
                    {
                     "name": "Varmail",
                     "nfiles": 2000,
                     "seconds": 2,
                     "num_ops": 60*MILLION,
                    },
                    {
                     "name": "Varmail",
                     "nfiles": 50,
                     "seconds": 2,
                     "num_ops": 60*MILLION,
                    },
                ]

            varmail_insts_long = [
                    {
                        "seconds": 600,
                        "nfiles": 20000,
                        "name": "Varmail",
                        "num_ops": 350000,
                        "do_strace": False,

                    },
                    {
                        "seconds": 600,
                        "nfiles": 500,
                        "name": "Varmail",
                        "num_ops": 350000,
                        "do_strace": False,
                    } ]

            para_dict.update( {
                    # 'ftl'              : ['dftldes', 'nkftl2'], <----
                    'ftl'              : ['nkftl2'],
                    'stripe_size'      : [64],
                    'enable_simulation': [True],
                    'dump_ext4_after_workload': [False],
                    'over_provisioning': [1.5], # 1.28 is a good number
                    'gc_high_ratio'    : [0.9],
                    'gc_low_ratio'     : [0.7],
                    'not_check_gc_setting': [False],
                    'cache_mapped_data_bytes' :[int(1*lbabytes)],
                    'segment_bytes'    : [4*MB],
                    'snapshot_interval': [1*SEC],
                    'write_gc_log'     : [False],
                    'ssd_ncq_depth'    : [32],
                    'max_log_blocks_ratio': [0.1],

                    'workload_class' : [ 'AppMix' ],
                    'run_seconds'    : [None],
                    'filesystem'     : ['ext4', 'f2fs', 'xfs'],

                    'appconfs': [
                            leveldb_insts_long,
                            sqlite_insts_long,
                            varmail_insts_long,
                            ]
                    })
            self.parameter_combs = ParameterCombinations(para_dict)

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        for para in ParaDict():
            print para
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
            expname = utils.get_expname()
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
            expname = utils.get_expname()
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



def simulate_from_event_files():
    def main():
        expname = utils.get_expname()
        trace_expnames = [
                 'rocksdb-reqscale',
                 'leveldb-reqscale-001',
                 'sqliteRB-reqscale-001',
                 'sqliteWAL-reqscale-001',
                 'varmail-reqscale-002'

                # 'samplerun2'
        ]
        rule = 'locality'
        # rule = 'alignment'

        for para in filesim.ParaDict(expname, trace_expnames, rule):
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = filesim.LocalExperimenter( Parameters(**para) )
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
            expname = utils.get_expname()

            pair_list = EventFilePairs('/tmp/results/64mbfile')

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





