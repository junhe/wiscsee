import copy
from environments import *
import itertools
import os
import pprint
import random

import WlRunner


def build_jobs(pattern_tuple, bs, usefs, conf, traffic_size, file_size):
    job = WlRunner.fio.JobDescription()
    # traffic_size = 1 * GB
    # traffic_size = 512 * KB

    if not usefs:
        global_sec =  {
                        'global': {
                            'ioengine'  : 'libaio',
                            'io_size'   : int(traffic_size),
                            'filesize'  : int(file_size),
                            'filename'  : '/dev/sdc',
                            'direct'    : 1,
                            'bs'        : int(bs)
                            }
                }
    else:
        # with filesystem
        global_sec =  {
                        'global': {
                            'ioengine'  : 'libaio',
                            'io_size'   : int(traffic_size),
                            'filesize'  : int(file_size),
                            'bs'        : int(bs),
                            'iodepth'   :1,
                            'fsync'     :1
                            }
                }
    job.add_section(global_sec)

    for i, pat in enumerate(pattern_tuple):
        # jobname = '-'.join(['JOB', "_".join(pattern_tuple), pat, str(i)])
        if not usefs:
            d = { pat:
                        {
                         'rw': pat,
                         'offset': i * traffic_size
                         # 'write_iolog': 'joblog.'+str(i)
                        }
                }
        else:
            # use file system
            d = { pat:
                        {
                         'rw': pat,
                         # 'write_iolog': 'joblog.'+str(i)
                         'filename': os.path.join(conf['fs_mount_point'],
                                        'fio.data.'+str(i))
                        }
                }

        job.add_section(d)

    return job

def build_a_set(blocksize, traffic_size, fs, dev_mb, file_size):
    patterns = ['read', 'write', 'randread', 'randwrite']
    two_ways = list(itertools.combinations_with_replacement(patterns, 2))
    patterns = [ (p, ) for p in patterns]
    patterns.extend(two_ways)

    # override
    patterns = [('randwrite', 'randwrite')]

    parameters = [ {'pattern': p} for p in patterns ]

    for para in parameters:
        para['bs'] = blocksize
        para['traffic_size'] = int(traffic_size)
        para['fs'] = fs
        para['dev_mb'] = dev_mb
        para['file_size'] = file_size

    return parameters

def build_patterns():
    parameters = []
    # for blocksize in [4*KB, 64*KB, 256*KB]:
    for blocksize in [64*KB]:
        for fs in ['ext4', 'f2fs']:
            for dev_mb in [1024]:
                for file_size in [256*MB]:
                    pattern_set = build_a_set(blocksize = blocksize,
                                              fs = fs,
                                              dev_mb = dev_mb,
                                              traffic_size = 1.5*GB,
                                              file_size = file_size
                                              )
                    parameters.extend(pattern_set)

    parameters = parameters * 2
    random.shuffle( parameters )
    pprint.pprint( parameters )

    return parameters


