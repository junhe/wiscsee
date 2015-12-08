import copy
from environments import *
import itertools
import os
import pprint
import random

import WlRunner


def build_jobs(pattern_tuple, bs, usefs, conf, traffic_size):
    job = WlRunner.fio.JobDescription()
    # traffic_size = 1 * GB
    # traffic_size = 512 * KB

    if not usefs:
        global_sec =  {
                        'global': {
                            'ioengine'  : 'libaio',
                            'size'      : traffic_size,
                            'filename'  : '/dev/sdc',
                            'direct'    : 1,
                            'bs'        : bs
                            }
                }
    else:
        global_sec =  {
                        'global': {
                            'ioengine'  : 'libaio',
                            'size'      : traffic_size,
                            'bs'        : bs
                            }
                }
    job.add_section(global_sec)

    for i, pat in enumerate(pattern_tuple):
        # jobname = '-'.join(['JOB', "_".join(pattern_tuple), pat, str(i)])
        d = { pat:
                    {
                     'rw': pat,
                     'offset': i * traffic_size
                     # 'write_iolog': 'joblog.'+str(i)
                    }
            }
        if usefs:
            d[pat]['filename'] = os.path.join(conf['fs_mount_point'],
                    'fio.data.'+str(i))
            del d[pat]['offset']

        job.add_section(d)

    return job

def build_a_set(blocksize, traffic_size, fs, dev_mb):
    patterns = ['read', 'write', 'randread', 'randwrite']
    two_ways = list(itertools.combinations_with_replacement(patterns, 2))
    patterns = [ (p, ) for p in patterns]
    patterns.extend(two_ways)

    parameters = [ {'pattern': p} for p in patterns ]

    for para in parameters:
        para['bs'] = blocksize
        para['traffic_size'] = traffic_size
        para['fs'] = fs
        para['dev_mb'] = dev_mb

    return parameters

def build_patterns():
    parameters = []
    for blocksize in [4*KB, 64*KB, 256*KB]:
        for fs in ['ext4', 'f2fs']:
            for dev_mb in [4096]:
                pattern_set = build_a_set(blocksize = blocksize,
                                          traffic_size = 1*GB,
                                          fs = fs,
                                          dev_mb = dev_mb
                                          )
                parameters.extend(pattern_set)

    parameters = parameters * 2
    random.shuffle( parameters )
    pprint.pprint( parameters )

    return parameters


