from commons import *

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


# To use, do
# para_dict.update( leveldb_aging )
leveldb_aging = {
        'age_workload_class': ['AppMix'],
        'aging_appconfs': [
                [
                    {'name' : 'LevelDB',
                     'benchmarks': 'overwrite,compact',
                     'num': 3*MILLION,
                     'max_key': 3*MILLION,
                     'max_log': -1,
                     'do_strace': False,
                     'use_existing_db': 0,
                    },
                    {'name' : 'LevelDB',
                     'benchmarks': 'fillseq,compact',
                     'num': 3*MILLION,
                     'max_key': 3*MILLION,
                     'max_log': -1,
                     'do_strace': False,
                     'use_existing_db': 0,
                    },

                ]
            ],
    }

leveldb_target = {
        'workload_class' : [ 'AppMix' ],
        'run_seconds'    : [None],
        'appconfs': [
                [ # list of app you want to run
                    {'name' : 'LevelDB',
                     'benchmarks': 'readrandom',
                     'num': 3*MILLION,
                     'max_key': 3*MILLION,
                     'max_log': -1,
                     'do_strace': False,
                     'use_existing_db': 1,
                     },
                    {'name' : 'LevelDB',
                     'benchmarks': 'readseq',
                     'num': 3*MILLION,
                     'max_key': 3*MILLION,
                     'max_log': -1,
                     'do_strace': False,
                     'use_existing_db': 1,
                     },

                ]
            ],
    }










