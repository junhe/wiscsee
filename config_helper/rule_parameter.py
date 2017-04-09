import os
import copy
import time

from experiment import *
from utilities import utils
from config import WLRUNNER, LBAGENERATOR, LBAMULTIPROC


class ParaDict(object):
    def __init__(self, expname, trace_expnames, rule):
        self.expname = expname
        self.trace_expnames = trace_expnames
        self.rule = rule

    def __iter__(self):
        expname = self.expname

        subexps = self.subexps(self.trace_expnames)

        print 'Number of subexps to simulate', len(subexps)
        time.sleep(1)

        for event_set in subexps:
            print event_set['mkfs_path']
            para_dict = event_set['original_config']['exp_parameters']
            para_dict.update({
                'mkfs_path': event_set['mkfs_path'],
                'ftlsim_path': event_set['ftlsim_path'],
                'expname': expname,

                })

            para_iter = self.get_para_iter(para_dict)
            for local_para_dict in para_iter:
                yield local_para_dict

    def get_para_iter(self, para_dict):
        if self.rule == 'locality':
            para_iter = LocalityParaIter(para_dict, coverage='large')

        elif self.rule == 'localitysmall':
            para_iter = LocalityParaIter(para_dict, coverage='small')

        elif self.rule == 'alignment':
            para_iter = AlignmentParaIter(para_dict)

        elif self.rule == 'grouping':
            para_iter = GroupingParaIter(para_dict)

        elif self.rule == 'integration':
            para_iter = IntegrationParaIter(para_dict)

        else:
            raise NotImplementedError(
                '{} not supported here.'.format(self.rule))

        return para_iter

    def subexps(self, expnames):
        subexp_sets = []
        for expname in expnames:
            path = os.path.join('/tmp/results', expname)
            sets = EventFileSets(path)
            subexp_sets.extend(sets.get_sets())

        return subexp_sets


class EventFileSets(object):
    """
    Given a dirpath, it returns a list. Each item in the list is
    a dictionary contain event files of mkfs, for-ftlsim, and config.
    """
    def __init__(self, dirpath):
        self.dirpath = dirpath

    def get_sets(self):
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

                    pairs.append(d)
        return pairs

    def _get_confjson(self, subexp_path):
        confpath = os.path.join(subexp_path, 'config.json')
        confjson = load_json(confpath)

        return confjson

class LocalityParaIter(object):
    """
    Given a para_dict (exp_parameters), generate a
    series of para_dict with different cache sizes
    """
    def __init__(self, para_dict, coverage='large'):
        self.para_dict = para_dict
        self.coverage = coverage

    def __iter__(self):
        if self.coverage == 'large':
            coverage_ratios = [0.1, 0.5, 1]
        elif self.coverage == 'small':
            coverage_ratios = [0.05, 0.01]

        for coverage_ratio in coverage_ratios:
            local_dict = copy.deepcopy(self.para_dict)

            lbabytes = local_dict['lbabytes']

            local_dict.update({
                'ftl': 'dftldes',
                'cache_mapped_data_bytes' :int(lbabytes * coverage_ratio),
                'ssd_ncq_depth'       : 1,
                'n_pages_per_block'   : 64,
                'stripe_size'         : 1,
                'enable_blktrace'     : False,
                'enable_simulation'   : True,
                'segment_bytes'       : 2*GB,
                'over_provisioning': 32, # 1.28 is a good number
                'gc_high_ratio'    : 0.9,
                'gc_low_ratio'     : 0.8,
                'snapshot_interval': 1*SEC,
                'write_gc_log'     : False,
                'wear_leveling_check_interval': 1000*SEC,
                'do_wear_leveling' : False,
                'snapshot_valid_ratios': False,
                'snapshot_erasure_count_dist': False,
                'n_channels_per_dev'  : 16,
                'do_gc_after_workload': False,
                'trace_issue_and_complete': False,
                'stop_sim_on_bytes': 1*GB,
                })

            yield local_dict

class AlignmentParaIter(object):
    """
    Given a para_dict (exp_parameters), generate a
    series of para_dict with different cache sizes
    """
    def __init__(self, para_dict):
        self.para_dict = para_dict

    def __iter__(self):
        local_dict = copy.deepcopy(self.para_dict)

        lbabytes = local_dict['lbabytes']

        for blocksize in [128*KB, 1*MB]:
            local_dict.update({
                'ftl': 'nkftl2',
                'ssd_ncq_depth'  : 1,
                'cache_mapped_data_bytes' :lbabytes,
                'n_pages_per_block': blocksize / (2*KB),
                'stripe_size'    : blocksize / (2*KB),
                'enable_blktrace': False,
                'enable_simulation': True,
                'segment_bytes'  : blocksize,   # isolate each block
                'max_log_blocks_ratio': 100, # never gc
                'over_provisioning': 8, # 1.28 is a good number
                'gc_high_ratio'    : 10, # never trigger gc
                'gc_low_ratio'     : 0.8,
                'not_check_gc_setting': True,
                'snapshot_interval': 10*SEC,
                'write_gc_log'     : False,
                'wear_leveling_check_interval': 100*SEC,
                'do_wear_leveling' : False,
                'snapshot_valid_ratios': False,
                'snapshot_erasure_count_dist': False,
                'n_channels_per_dev'  : 16,
                'do_gc_after_workload': True,
                'stop_sim_on_bytes': 100*GB,
                'log_group_factor': 100000,
                'trace_issue_and_complete': False,
                })

            yield local_dict


class GroupingParaIter(object):
    def __init__(self, para_dict):
        self.para_dict = para_dict

    def __iter__(self):
        local_dict = copy.deepcopy(self.para_dict)

        lbabytes = local_dict['lbabytes']

        for segment_bytes in [128*MB, 2*GB]:
            local_dict.update({
                'ftl': 'dftldes',
                'ssd_ncq_depth'  : 1,
                'cache_mapped_data_bytes' :lbabytes,
                'n_pages_per_block': 64,
                'stripe_size'    : 1,
                'enable_blktrace': False,
                'enable_simulation': True,
                'segment_bytes'  : segment_bytes,
                'over_provisioning': 8, # 1.28 is a good number
                'gc_high_ratio'    : 0.9,
                'gc_low_ratio'     : 0.8,
                'not_check_gc_setting': True,
                'snapshot_interval': 0.1*SEC,
                'write_gc_log'     : False,
                'wear_leveling_check_interval': 100*SEC,
                'do_wear_leveling' : False,
                'snapshot_valid_ratios': True,
                'snapshot_erasure_count_dist': False,
                'n_channels_per_dev'  : 16,
                'do_gc_after_workload': False,
                'stop_sim_on_bytes': 100*GB,
                'trace_issue_and_complete': False,
                })

            yield local_dict


class IntegrationParaIter(object):
    """
    It simulates a block trace on realistic FTL.
    """
    def __init__(self, para_dict):
        self.para_dict = para_dict
        self.lbabytes = para_dict['lbabytes']

    def __iter__(self):
        yield self.get_for_dftl()
        yield self.get_for_nkftl()

    def get_for_dftl(self):
        local_dict = copy.deepcopy(self.para_dict)
        local_dict.update({
            'ftl': 'dftldes',
            'ssd_ncq_depth'  : 1,  #<--------------------------------has to set
            'dirty_bytes'    : None,
            'cache_mapped_data_bytes' : 0.1 * self.lbabytes,
            'n_pages_per_block': 64,
            'stripe_size'    : 1,
            'enable_blktrace': False,
            'enable_simulation': True,
            'segment_bytes'  : 128*MB,
            'over_provisioning': 1.50, # 1.28 is a good number
            'gc_high_ratio'    : 0.9,
            'gc_low_ratio'     : 0.8,
            'not_check_gc_setting': True,
            'snapshot_interval': 0.1*SEC,
            'write_gc_log'     : False,
            'wear_leveling_check_interval': 100*SEC,
            'do_wear_leveling' : False,
            'snapshot_valid_ratios': True,
            'snapshot_erasure_count_dist': False,
            'n_channels_per_dev'  : 16,
            'do_gc_after_workload': False,
            'stop_sim_on_bytes': 'inf',
            'trace_issue_and_complete': False,
            })

        return local_dict

    def get_for_nkftl(self):
        local_dict = copy.deepcopy(self.para_dict)
        local_dict.update({
            'ftl'            : 'nkftl2',
            'dirty_bytes'    : None,
            'ssd_ncq_depth'  : 1,  #<--------------------------------has to set
            'n_pages_per_block': 64, # 128KB block
            'stripe_size'    : 64, # stripe size is the block size
            'enable_blktrace': False,
            'enable_simulation': True,
            'f2fs_gc_after_workload': False,
            'segment_bytes'  : 1*MB, # thus N=8
            'max_log_blocks_ratio': 0.07,
            'n_online_cpus'  : 'all',
            'over_provisioning': 1.5, # 1.28 is a good number
            'gc_high_ratio'    : 0.9,
            'gc_low_ratio'     : 0.8,
            'not_check_gc_setting': True,
            'snapshot_interval': 0.1*SEC,
            'write_gc_log'     : False,
            'wear_leveling_check_interval': 100*SEC,
            'do_wear_leveling' : False,
            'wear_leveling_factor': 2,
            'wear_leveling_diff': 10,
            'snapshot_valid_ratios': True,
            'snapshot_erasure_count_dist': False,
            'n_channels_per_dev'  : 16,
            'do_gc_after_workload': False,
            'trace_issue_and_complete': False,
            'age_workload_class': 'NoOp',
            'aging_appconfs': None,
            'stop_sim_on_bytes': 'inf',
            'log_group_factor': 10, # So N=8, K=80. max log blocks =800
            })

        return local_dict


