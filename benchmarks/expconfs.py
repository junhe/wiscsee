import pprint

from commons import *
from experimenter import get_shared_para_dict
from utilities.utils import *

# LEVELDB
#   k written_bytes valid_data_bytes   num max_key   hotness
# 1 1     2.5875816       0.10041694 6e+06   6e+05 25.768377
# 2 2     4.1530533       0.49622203 6e+06   6e+06  8.369345
# 3 3     0.9941254       0.01778593 6e+06   6e+04 55.893923
# 4 4     0.4081306       0.01940136 3e+06   3e+04 21.036185      20+sec
# 5 5     1.0261040       0.06891455 3e+06   3e+05 14.889512
# 6 6     1.6484642       0.23112959 3e+06   3e+06  7.132208      20+ sec
# 7 1     1.6486170       0.23750480 3e+06   3e+06  6.941405
# 8 2     2.5417020       0.30242460 4e+06   4e+06  8.404416
# 9 3     2.3927960       0.27410040 4e+06   3e+06  8.729633

def repeat_bench(name, n):
    return ','.join([name] * n)


proc_settings = {
    ######## LevelDB #######
    'leveldb': {
        'aging_overwrite':
            {'name' : 'LevelDB',
             'benchmarks': 'overwrite,compact',
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 1*GB,
            },

        'aging_fillseq':
            {'name' : 'LevelDB',
             'benchmarks': 'fillseq,compact',
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 1*GB,
            },

        'readrandom':
            {'name' : 'LevelDB',
             'benchmarks': repeat_bench('readrandom', 10),
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 1,
             'mem_limit_in_bytes': 128*MB,
             },

        'readseq':
            {'name' : 'LevelDB',
             'benchmarks': repeat_bench('readseq', 10),
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 1,
             'mem_limit_in_bytes': 128*MB,
             },

        'writeseq':
            {'name' : 'LevelDB',
             'benchmarks': repeat_bench('fillseq', 1),
             'num': 10*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
             },

        'writerandom':
            {'name' : 'LevelDB',
             'benchmarks': repeat_bench('overwrite', 1),
             'num': 10*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
            },

        'writeseq_for_mix':
            {'name' : 'LevelDB',
             'benchmarks': repeat_bench('fillseq', 1),
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
             },

        'writerandom_for_mix':
            {'name' : 'LevelDB',
             'benchmarks': repeat_bench('overwrite', 1),
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
            },

    }, ### LevelDB

    ######## RocksDB #######
    'rocksdb': {
        'aging_overwrite':
            {'name' : 'RocksDB',
             'benchmarks': 'overwrite,compact',
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 1*GB,
            },

        'aging_fillseq':
            {'name' : 'RocksDB',
             'benchmarks': 'fillseq,compact',
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 1*GB,
            },

        'readrandom':
            {'name' : 'RocksDB',
             'benchmarks': repeat_bench('readrandom', 10),
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 1,
             'mem_limit_in_bytes': 128*MB,
             },

        'readseq':
            {'name' : 'RocksDB',
             'benchmarks': repeat_bench('readseq', 10),
             'num': 5*MILLION,
             'do_strace': False,
             'use_existing_db': 1,
             'mem_limit_in_bytes': 128*MB,
             },

        'writeseq':
            {'name' : 'RocksDB',
             'benchmarks': repeat_bench('fillseq', 1),
             'num': 10*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
             },

        'writerandom':
            {'name' : 'RocksDB',
             'benchmarks': repeat_bench('overwrite', 1),
             'num': 10*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
            },

        'writeseq_for_mix':
            {'name' : 'RocksDB',
             'benchmarks': repeat_bench('fillseq', 1),
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
             },

        'writerandom_for_mix':
            {'name' : 'RocksDB',
             'benchmarks': repeat_bench('overwrite', 1),
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 128*MB,
            },

    }, ### RocksDB

    ######## SqliteWAL #######
    'sqliteWAL': {
        'aging_seq':
            {'name': 'Sqlite',
             'pattern': 'sequential_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'WAL'
            },

        'aging_rand':
            {'name': 'Sqlite',
             'pattern': 'random_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'WAL'
            },

        'seq_get':
            {'name': 'Sqlite',
             'pattern': 'sequential_get',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'WAL'
            },

        'rand_get':
            {'name': 'Sqlite',
             'pattern': 'random_get',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'WAL'
            },

        'seq_put':
            {'name': 'Sqlite',
             'pattern': 'sequential_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'WAL'
            },

        'rand_put':
            {'name': 'Sqlite',
             'pattern': 'random_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'WAL'
            },
    }, ### Sqlite

    ######## SqliteRollBack #######
    'sqliteRB': {
        'aging_seq':
            {'name': 'Sqlite',
             'pattern': 'sequential_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'DELETE'
            },

        'aging_rand':
            {'name': 'Sqlite',
             'pattern': 'random_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'DELETE'
            },

        'seq_get':
            {'name': 'Sqlite',
             'pattern': 'sequential_get',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'DELETE'
            },

        'rand_get':
            {'name': 'Sqlite',
             'pattern': 'random_get',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'DELETE'
            },

        'seq_put':
            {'name': 'Sqlite',
             'pattern': 'sequential_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'DELETE'
            },

        'rand_put':
            {'name': 'Sqlite',
             'pattern': 'random_put',
             'n_insertions': 12000,
             'commit_period': 10,
             'max_key': 12000,
             'do_strace': False,
             'journal_mode': 'DELETE'
            },
    }, ### Sqlite


    ######## Varmail  #######
    'varmail': {
        'aging_small':
                    {
                        "seconds": 600,
                        "nfiles": 1000,
                        "name": "Varmail",
                        "num_bytes": 1*MB,
                        "do_strace": False,
                        "rwmode": 'read', # even it is read, it preallocates files
                        'mem_limit_in_bytes': 1*GB,
                    },

        'aging_large':
                    {
                        "seconds": 600,
                        "nfiles": 8000,
                        "name": "Varmail",
                        "num_bytes": 1*MB,
                        "do_strace": False,
                        'mem_limit_in_bytes': 1*GB,
                        "rwmode": 'read', # even it is read, it preallocates files
                    },

        'small_read':
                    {
                        "seconds": 600,
                        "nfiles": 800,
                        "name": "Varmail",
                        "num_bytes": 512*MB,
                        "do_strace": False,
                        'mem_limit_in_bytes': 64*MB,
                        "rwmode": 'read'
                    },

        'large_read':
                    {
                        "seconds": 600,
                        "nfiles": 8000,
                        "name": "Varmail",
                        "num_bytes": 512*MB,
                        "do_strace": False,
                        'mem_limit_in_bytes': 64*MB,
                        "rwmode": 'read'
                    },

        'small_write':
                    {
                        "seconds": 600,
                        "nfiles": 800,
                        "name": "Varmail",
                        "num_bytes": 1*GB,
                        "do_strace": False,
                        'mem_limit_in_bytes': 1*GB,
                        "rwmode": 'write'
                    },

        'large_write':
                    {
                        "seconds": 600,
                        "nfiles": 8000,
                        "name": "Varmail",
                        "num_bytes": 1*GB,
                        "do_strace": False,
                        'mem_limit_in_bytes': 1*GB,
                        "rwmode": 'write'
                    },
    }, ### Sqlite
}



class ParameterPool(object):
    def __init__(self, expname, testname, filesystem):
        self.lbabytes = 1 * GB
        self.expname = expname
        self.filesystem = filesystem

        self.para_dicts = []

        for name in testname:
            func = eval('self.{}'.format(name))
            func(name)

    def __iter__(self):
        for para_dict in self.para_dicts:
            yield para_dict

    def env_reqscale(self, d):
        d.update(
            {
                'ftl' : ['ftlcounter'],
                'enable_simulation': [True],
                'dump_ext4_after_workload': [True],
                'only_get_traffic': [False],
                'do_ncq_depth_time_line': [True],
            })

    def get_base_dict(self):
        shared_para_dict = get_shared_para_dict(
                self.expname, self.lbabytes)
        shared_para_dict['filesystem'] = self.filesystem

        return shared_para_dict

    def extend_para_dicts(self, para_dicts):
        self.para_dicts.extend(para_dicts)

    def set_testname(self, d, name):
        d.update( {'testname': [name]} )



    def rocksdb_reqscale_r_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['rocksdb']['aging_fillseq']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['readseq']
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def rocksdb_reqscale_r_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['rocksdb']['aging_overwrite']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['readrandom'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def rocksdb_reqscale_r_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['rocksdb']['aging_fillseq'],
                        proc_settings['rocksdb']['aging_overwrite']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['readseq'],
                        proc_settings['rocksdb']['readrandom'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def rocksdb_reqscale_w_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['writeseq'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def rocksdb_reqscale_w_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['writerandom'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def rocksdb_reqscale_w_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [40],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['writeseq_for_mix'],
                        proc_settings['rocksdb']['writerandom_for_mix'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))



    def leveldb_reqscale_r_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['leveldb']['aging_fillseq']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['leveldb']['readseq']
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def leveldb_reqscale_r_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['leveldb']['aging_overwrite']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['leveldb']['readrandom'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def leveldb_reqscale_r_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['leveldb']['aging_fillseq'],
                        proc_settings['leveldb']['aging_overwrite']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['leveldb']['readseq'],
                        proc_settings['leveldb']['readrandom'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def leveldb_reqscale_w_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['leveldb']['writeseq'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def leveldb_reqscale_w_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['leveldb']['writerandom'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def leveldb_reqscale_w_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [40],
            'appconfs': [
                    [
                        proc_settings['leveldb']['writeseq_for_mix'],
                        proc_settings['leveldb']['writerandom_for_mix'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))




    def sqliteWAL_reqscale_r_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['sqliteWAL']['aging_seq']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL']['seq_get']
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def sqliteWAL_reqscale_r_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['sqliteWAL']['aging_rand']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL']['rand_get'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def sqliteWAL_reqscale_r_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['sqliteWAL']['aging_seq'],
                        proc_settings['sqliteWAL']['aging_rand']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL']['seq_get'],
                        proc_settings['sqliteWAL']['rand_get'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def sqliteWAL_reqscale_w_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL']['seq_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def sqliteWAL_reqscale_w_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL']['rand_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def sqliteWAL_reqscale_w_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL']['seq_put'],
                        proc_settings['sqliteWAL']['rand_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def sqliteRB_reqscale_r_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['sqliteRB']['aging_seq']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteRB']['seq_get']
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def sqliteRB_reqscale_r_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['sqliteRB']['aging_rand']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteRB']['rand_get'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def sqliteRB_reqscale_r_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['sqliteRB']['aging_seq'],
                        proc_settings['sqliteRB']['aging_rand']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteRB']['seq_get'],
                        proc_settings['sqliteRB']['rand_get'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def sqliteRB_reqscale_w_seq(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['sqliteRB']['seq_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def sqliteRB_reqscale_w_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['sqliteRB']['rand_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def sqliteRB_reqscale_w_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [10],
            'appconfs': [
                    [
                        proc_settings['sqliteRB']['seq_put'],
                        proc_settings['sqliteRB']['rand_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def varmail_reqscale_r_small(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['varmail']['aging_small']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['varmail']['small_read']
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def varmail_reqscale_r_large(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['varmail']['aging_large']
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['varmail']['large_read'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def varmail_reqscale_r_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        shared_para_dict.update({
            'age_workload_class': ['AppMix'],
            'aging_appconfs': [
                    [
                        proc_settings['varmail']['aging_small'],
                        proc_settings['varmail']['aging_large'],
                    ]
                ],
        })

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['varmail']['small_read'],
                        proc_settings['varmail']['large_read'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))
        pprint.pprint( self.para_dicts )

    def varmail_reqscale_w_small(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['varmail']['small_write'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def varmail_reqscale_w_large(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['varmail']['large_write'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))

    def varmail_reqscale_w_mix(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['varmail']['small_write'],
                        proc_settings['varmail']['large_write'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))


    def tmptest_na_na_na(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_reqscale(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'enable_simulation': [False],
            'appconfs': [
                    [
                        {'name' : 'LevelDB',
                         'benchmarks': repeat_bench('overwrite', 1),
                         'num': 10000,
                         'do_strace': False,
                         'use_existing_db': 0,
                         'mem_limit_in_bytes': 1*GB,
                        },
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))



