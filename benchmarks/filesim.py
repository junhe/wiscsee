import os
import copy
import time

from experimenter import *
from utilities import utils
from config import WLRUNNER, LBAGENERATOR, LBAMULTIPROC


"""
This module is used to read event file and simulate those events
"""


class LocalExperimenter(Experimenter):
    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR

        self.conf["lba_workload_class"] = "BlktraceEvents"

        self.conf['lba_workload_configs']['mkfs_event_path'] = \
                self.para.mkfs_path
        self.conf['lba_workload_configs']['ftlsim_event_path'] = \
                self.para.ftlsim_path

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
            para_iter = LocalityParaIter(para_dict)
        elif self.rule == 'alignment':
            para_iter = AlignmentParaIter(para_dict)
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
    def __init__(self, para_dict):
        self.para_dict = para_dict

    def __iter__(self):
        for coverage_ratio in [0.1, 0.5, 1]:
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
                'do_ncq_depth_time_line': False,
                'stop_sim_on_bytes': 512*MB,
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

        local_dict.update({
            'ftl': 'nkftl2',
            'ssd_ncq_depth'  : 1,
            'cache_mapped_data_bytes' :lbabytes,
            'n_pages_per_block': 64,
            'stripe_size'    : 64,
            'enable_blktrace': False,
            'enable_simulation': True,
            'segment_bytes'  : 128*KB,   # isolate each block
            'max_log_blocks_ratio': 100, # never gc
            'over_provisioning': 8, # 1.28 is a good number
            'gc_high_ratio'    : 0.9,
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
            'stop_sim_on_bytes': 1*GB,
            'log_group_factor': 100000,
            })

        yield local_dict


class GroupingParaIter(object):
    def __init__(self, para_dict):
        self.para_dict = para_dict

    def __iter__(self):
        local_dict = copy.deepcopy(self.para_dict)

        lbabytes = local_dict['lbabytes']

        local_dict.update({
            'ftl': 'dftldes',
            'ssd_ncq_depth'  : 1,
            'cache_mapped_data_bytes' :lbabytes,
            'n_pages_per_block': 64,
            'stripe_size'    : 1,
            'enable_blktrace': False,
            'enable_simulation': True,
            'segment_bytes'  : 128*MB,
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
            })

        yield local_dict


