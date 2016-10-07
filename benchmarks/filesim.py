import os
import copy

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
    def __iter__(self):
        expname = utils.get_expname()

        subexps = self.subexps(['1gb'])

        for event_set in subexps:
            print event_set['mkfs_path']
            para_dict = event_set['original_config']['exp_parameters']
            para_dict.update({
                'mkfs_path': event_set['mkfs_path'],
                'ftlsim_path': event_set['ftlsim_path'],
                'expname': expname,

                })
            para_iter = LocalityParaIter(para_dict)
            for local_para_dict in para_iter:
                yield local_para_dict

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
                'cache_mapped_data_bytes' :lbabytes * coverage_ratio,
                })

            yield local_dict



