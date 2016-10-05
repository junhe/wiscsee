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


proc_settings = {
    ######## LevelDB #######
    'leveldb': {

        'aging_overwrite':
            {'name' : 'LevelDB',
             'benchmarks': 'overwrite,compact',
             'num': 3*MILLION,
             'max_key': 3*MILLION,
             'max_log': -1,
             'do_strace': False,
             'use_existing_db': 0,
            },

        'aging_fillseq':
            {'name' : 'LevelDB',
             'benchmarks': 'fillseq,compact',
             'num': 3*MILLION,
             'max_key': 3*MILLION,
             'max_log': -1,
             'do_strace': False,
             'use_existing_db': 0,
            },

        'readrandom':
            {'name' : 'LevelDB',
             'benchmarks': 'readrandom',
             'num': 3*MILLION,
             'max_key': 3*MILLION,
             'max_log': -1,
             'do_strace': False,
             'use_existing_db': 1,
             },

        'readseq':
            {'name' : 'LevelDB',
             'benchmarks': 'readseq',
             'num': 3*MILLION,
             'max_key': 3*MILLION,
             'max_log': -1,
             'do_strace': False,
             'use_existing_db': 1,
             },
    }, ### LevelDB

    ######## RocksDB #######
    'rocksdb': {
        'aging_overwrite':
            {'name' : 'RocksDB',
             'benchmarks': 'overwrite,compact',
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 1*GB,
            },

        'aging_fillseq':
            {'name' : 'RocksDB',
             'benchmarks': 'fillseq,compact',
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 0,
             'mem_limit_in_bytes': 1*GB,
            },

        'readrandom':
            {'name' : 'RocksDB',
             'benchmarks': 'readrandom',
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 1,
             },

        'readseq':
            {'name' : 'RocksDB',
             'benchmarks': 'readseq',
             'num': 3*MILLION,
             'do_strace': False,
             'use_existing_db': 1,
             },
    }, ### RocksDB

}


class ParameterPool(object):
    def __init__(self, expname, testname, filesystem):
        lbabytes = 1*GB
        shared_para_dict = get_shared_para_dict(expname, lbabytes)
        shared_para_dict['filesystem'] = filesystem

        func = eval('self.{}'.format(testname))
        func(shared_para_dict)

    def __iter__(self):
        for para_dict in self.para_dicts:
            yield para_dict

    def env_reqscale_read(self, d):
        d.update(
            {
                'ftl' : ['ftlcounter'],
                'enable_simulation': [True],
                'dump_ext4_after_workload': [True],
                'only_get_traffic': [False],
                'do_ncq_depth_time_line': [True],
            })






    def rocksdb_reqscale_r_seq(self, shared_para_dict):
        self.env_reqscale_read(shared_para_dict)

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
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['rocksdb']['readseq']
                    ]
                ],
        })

        self.para_dicts = ParameterCombinations(shared_para_dict)
        pprint.pprint( self.para_dicts )


    def leveldb_reqscale_r_seq(self, shared_para_dict):
        self.env_reqscale_read(shared_para_dict)

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
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['leveldb']['readseq']
                    ]
                ],
        })

        self.para_dicts = ParameterCombinations(shared_para_dict)
        pprint.pprint( self.para_dicts )















