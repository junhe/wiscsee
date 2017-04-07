import pprint

from commons import *
from experimenter import get_shared_para_dict
from utilities.utils import *

def repeat_bench(name, n):
    return ','.join([name] * n)


proc_settings = {
    ######## SqliteWAL #######
    'sqliteWAL': {
        'rand_put':
            {'name': 'Sqlite',
             'pattern': 'random_put',
             'n_insertions': 40000, #240000=1.8GB
             'commit_period': 10,
             'max_key': 240000,
             'do_strace': False,
             'mem_limit_in_bytes': 32*MB,
             'journal_mode': 'WAL'
            },
    }, ### Sqlite

    ######## SqliteWAL #######
    'sqliteWAL-wear': {
        'rand_put':
            {'name': 'Sqlite',
             'pattern': 'random_put',
             'n_insertions': 40000,
             'commit_period': 10,
             'max_key': 240000,
             'do_strace': False,
             'mem_limit_in_bytes': 1*GB,
             'journal_mode': 'WAL'
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
                'trace_issue_and_complete': [True],
            })

    def env_wearlevel(self, d):
        d.update(
            {
                'ftl' : ['ftlcounter'],
                'device_path'    : ['/dev/loop0'],
                'enable_simulation': [True],
                'dump_ext4_after_workload': [True],
                'only_get_traffic': [False],
                'trace_issue_and_complete': [False],
                'gen_ncq_depth_table': [False],
                'do_dump_lpn_sem': [False],
                'rm_blkparse_events': [True],
                'sort_block_trace': [False],
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

    def sqliteWAL_wearlevel_w_rand(self, testname):
        shared_para_dict = self.get_base_dict()
        self.env_wearlevel(shared_para_dict)
        self.set_testname(shared_para_dict, testname)

        # set aging
        # Do nothing.

        # set target
        shared_para_dict.update({
            'workload_class' : [ 'AppMix' ],
            'run_seconds'    : [None],
            'appconfs': [
                    [
                        proc_settings['sqliteWAL-wear']['rand_put'],
                    ]
                ],
        })

        self.extend_para_dicts(ParameterCombinations(shared_para_dict))



