# To use, do
# para_dict.update( leveldb_aging )
leveldb_aging = {
        'age_workload_class': ['AppMix'],
        'aging_appconfs': [
                [
                    {'name' : 'LevelDB',
                     'benchmarks': 'fillseq',
                     'num': 1*100000,
                     'max_key': 1*100000,
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
                     'num': 1*100000,
                     'max_key': 1*100000,
                     'max_log': -1,
                     'do_strace': False,
                     'use_existing_db': 1,
                     },
                ]
            ],
    }


