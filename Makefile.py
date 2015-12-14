#!/usr/bin/env python
import copy
import itertools
import random
import argparse
import re
import subprocess
import os
import profile
import sys
import shlex
import socket
import time
import glob
import pprint
from time import localtime, strftime

import config
from config import WLRUNNER, LBAGENERATOR
from environments import *
import experiment
import testfio
import FtlSim
import WlRunner
from utils import *


def save_conf(conf):
    confpath = os.path.join(conf['result_dir'], 'config.json')
    prepare_dir_for_path(confpath)
    conf.dump_to_file(confpath)


def workflow(conf):
    # save config file for reference
    save_conf(conf)

    # run the workload
    workload_src = conf['workload_src']
    if workload_src == WLRUNNER:
        runner = WlRunner.wlrunner.WorkloadRunner(conf)
        event_iter = runner.run()
    elif workload_src == LBAGENERATOR:
        lbagen = eval("""WlRunner.lbaworkloadgenerator.{classname}(conf)""".\
            format(classname=conf['lba_workload_class']))
        event_iter = lbagen
    else:
        raise RuntimeError("{} is not a valid workload source"\
            .format(workload_src))

    if not conf['enable_blktrace'] or not conf['enable_simulation']:
        return

    # run the Ftl Simulator
    print "Start simulation.........."
    sim = FtlSim.simulator.Simulator(conf)
    sim.run(event_iter)

def get_default_config():
    MOpt = WlRunner.filesystem.MountOption

    confdic = {
        ############### Global #########
        "result_dir"            : None,
        "workload_src"          : WLRUNNER,
        # "workload_src"          : LBAGENERATOR,
        "expname"               : "default-expname",
        "time"                  : None,
        "subexpname"            : "default-subexp",
        # directmap, blockmap, pagemap, hybridmap, dftl2, tpftl, nkftl
        "ftl_type"              : "nkftl2",
        "sector_size"           : 512,


        ############## For FtlSim ######
        "enable_simulation"     : True,
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 4,
        "flash_num_blocks"      : None,
        "enable_e2e_test"       : False,

        ############## Dftl ############
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 4, # 32 bits
            "GC_threshold_ratio": 0.95,
            "GC_low_threshold_ratio": 0.9,
            "over_provisioning": 1.28,
            "max_cmt_bytes": None, # cmt: cached mapping table
            "tpftl": {
                "entry_node_bytes": 6, # page 8, TPFTL paper
                "page_node_bytes": 8,  # m_vpn, pointer to entrylist
                "selective_threshold": 3
            }
        },

        ############## NKFTL (SAST) ############
        "nkftl": {
            'n_blocks_in_data_group': 4, # number of blocks in a data block group
            'max_blocks_in_log_group': 2, # max number of blocks in a log block group

            "GC_threshold_ratio": 0.8,
            "GC_low_threshold_ratio": 0.7,

            "provision_ratio": 1.5 # 1.5: 1GB user size, 1.5 flash size behind
        },

        ############## hybridmap ############
        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "hybridmapftl": {
            "low_log_block_ratio": 0.32
        },

        ############## recorder #############
        "verbose_level" : -1,
        "output_target" : "file",
        "print_when_finished": False,
        # "output_target" : "stdout",
        "record_bad_victim_block": False,

        ############## For WlRunner ########
        # for loop dev
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",

        "device_path"           : "/dev/sdc1", # or sth. like /dev/sdc1
        "device_type"           : "real", # loop, real
        # "device_path"           : "/dev/loop0", # or sth. like /dev/sdc1
        # "device_type"           : "loop", # loop, real

        "enable_blktrace"       : False,

        "fs_mount_point"        : "/mnt/fsonloop",
        "mnt_opts" : {
            "ext4":   { 'discard': MOpt(opt_name = "discard",
                                         value = "discard",
                                         include_name = False),
                        'data': MOpt(opt_name = "data",
                                        value = "ordered",
                                        include_name = True) },
            "btrfs":  { "discard": MOpt(opt_name = "discard",
                                         value = "discard",
                                         include_name = False),
                                         "ssd": MOpt(opt_name = 'ssd',
                                             value = 'ssd',
                                     include_name = False),
                        "autodefrag": MOpt(opt_name = 'autodefrag',
                                            value = 'autodefrag',
                                            include_name = False) },
            "xfs":    {'discard': MOpt(opt_name = 'discard',
                                        value = 'discard',
                                        include_name = False)},
            "f2fs":   {'discard': MOpt(opt_name = 'discard',
                                        value = 'discard',
                                        include_name = False)}
        },
        # "common_mnt_opts"       : ["discard", "nodatacow"],
        "filesystem"            : None,

        ############## FS ##################
        "ext4" : {
            "make_opts": {'-O':['^uninit_bg'], '-b':[4096]}
        },
        "f2fs"  : {"make_opts": {}, 'sysfs':{}},
        "btrfs"  : {"make_opts": {}},


        ############## workload.py workload to age FS ###
        # This is run after mounting the file and before real workload
        # Having this specific aging workload is because we don't want
        # its performance statistics to be recorded.
        "age_workload_class"    : "NoOp",

        # the following config should match the age_workload_class you use
        "aging_config_key"      :None,
        "aging_config" :{
            "generating_func": "self.generate_random_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": 4 * 2**20 / (512 * 1024),
            "chunk_size" : 512 * 1024,
            "iterations" : 1,
            "filename"   : "aging.file",
            "n_col"      : 5   # only for hotcold workload
        },


        ############## workload.py on top of FS #########
        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "workload_conf_key"     : "workload_conf",
        "workload_conf" :{
            # "generating_func": "self.generate_hotcold_workload",
            # "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            "generating_func": "self.generate_random_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": 4 * 2**20 / (512 * 1024),
            "chunk_size" : 512 * 1024,
            "iterations" : 1,
            "n_col"      : 5,   # only for hotcold workload
            "filename"   : "test.file"
        },

        ############## LBAGENERATOR  #########
        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        # "lba_workload_class"    : "Sequential",
        # "lba_workload_class"    : "HotCold",
        # "lba_workload_class"    : "Random",
        "lba_workload_class"    : "Manual",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.05,
            "write_to_lba_ratio"     : 1,    #how many writes you want to have
            "HotCold": {
                'chunk_bytes': 4096 * 1024,
                'chunk_count': 4,
                'n_col'      : 4
            }
        },

        ############# PERF #####################
        "wrap_by_perf" : False,
        "perf" : {
                "perf_path"         : "perf",
                "flamegraph_dir"    : None
                },

        ############# OS #####################
        "linux_version": linux_kernel_version(),
        "n_online_cpus": 1
    }
    return confdic

def reproduce():
    conf = config.Config()
    conf.load_from_json_file('/tmp/results/100iter-0.7low/default-subexp-f2fs-10-19-14-15-08--3121215361837401485/config.json')

    # Comment out the following lines if you don't want to reset the target dir
    # metadata_dic = choose_exp_metadata(conf)
    # conf.update(metadata_dic)
    # runtime_update(conf)
    # pprint.pprint(conf)

    workflow(conf)

def simple_lba_test():
    confdic = get_default_config()
    conf = config.Config(confdic)

    loop_dev_mb = 1024

    metadata_dic = {}
    metadata_dic['workload_src'] = LBAGENERATOR
    metadata_dic['targetdir'] = '/tmp/results'
    metadata_dic['filesystem'] = 'none'
    metadata_dic['loop_dev_size_mb'] = loop_dev_mb
    conf.update(metadata_dic)

    conf['flash_npage_per_block'] = 32
    conf['nkftl']['n_blocks_in_data_group'] = 2
    conf['nkftl']['max_blocks_in_log_group'] = 4
    conf['nkftl']['provision_ratio'] = 1.5
    conf['nkftl']['GC_threshold_ratio'] = 0.8
    conf['nkftl']['GC_low_threshold_ratio'] = 0.7
    conf['enable_e2e_test'] = True
    conf.set_flash_num_blocks_by_bytes(
        int((loop_dev_mb * 2**20) * conf['nkftl']['provision_ratio']))
    # conf.nkftl_set_flash_num_blocks_by_data_block_bytes(4 * 2**20)
    print conf
    runtime_update(conf)
    workflow(conf)

def simple_lba_test_dftl():
    confdic = get_default_config()
    conf = config.Config(confdic)

    # Experiment metadata
    metadata_dic = {}
    metadata_dic['workload_src'] = LBAGENERATOR
    metadata_dic['targetdir'] = '/tmp/results'
    metadata_dic['filesystem'] = 'none'
    metadata_dic["lba_workload_class"] = "Manual"
    conf.update(metadata_dic)

    # DFTL config
    dftl_update = {
        "ftl_type": "dftl2",
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 4, # 32 bits
            "GC_threshold_ratio": 0.8,
            "GC_low_threshold_ratio": 0.6,
            "over_provisioning": 1.28,
            "max_cmt_bytes": None, # cmt: cached mapping table
        }
    }
    conf.update(dftl_update)

    devsize_mb = loop_dev_mb = 32

    conf['flash_npage_per_block'] = 32
    conf['enable_e2e_test'] = True

    # More DFTL
    entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])
    conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
    conf['interface_level'] =  'page'
    conf['loop_dev_size_mb'] = devsize_mb
    conf['filesystem'] =  'f2fs'

    to_bytes = int((loop_dev_mb * MB) * conf['dftl']['over_provisioning'])
    conf.set_flash_num_blocks_by_bytes(to_bytes)

    runtime_update(conf)

    workflow(conf)

def test_nkftl():
    """
    """
    confdic = get_default_config()
    conf = config.Config(confdic)

    metadata_dic = choose_exp_metadata(conf)
    conf.update(metadata_dic)

    age_filesize = 400 * MB
    age_chunksize = 64 * KB

    conf["age_workload_class"] = "NoOp"
    aging_update = {
        "aging_config" :{
            # "generating_func": "self.generate_random_workload",
            "generating_func": "self.generate_mix_seq_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": age_filesize / age_chunksize,
            "chunk_size" : age_chunksize,
            "iterations" : 1,
            "num_files" : 2
        }
    }
    conf.update(aging_update)

    nks = [
            # {'N': 1024, 'K': 1024},
            # {'N': 4, 'K': 2**30},
            {'N': 1, 'K': 1}
            # {'N': 4, 'K': 8},
            # {'N': 8, 'K': 4},
            # {'N': 4, 'K': 4},
            # {'N': 'nflashblocks', 'K': 2**30}
            ]
    loop_dev_mb = 1 * 1024   # <--------------------- Set it?
    # bytes_to_write = 20 * 2**30
    bytes_to_write = 6 * GB

    for nk in nks:
        N = nk['N']
        K = nk['K']
        # for fs in ('f2fs', 'ext4'):
        for fs in ('f2fs', 'ext4'):
            # for chunksize in (16 * 1024, 64 * 1024, 512 * 1024):
            for chunksize in (16 * KB,):
                # for filesize in [ 2**i * 32*2**20 for i in range(0, 5, 1) ]:
                for filesize in [ 512 * MB ]:

                    # this will garantee that file system writes is confined
                    # in this space
                    conf['loop_dev_size_mb'] = loop_dev_mb
                    conf.set_flash_num_blocks_by_bytes(
                        int((loop_dev_mb * 2**20) * conf['nkftl']['provision_ratio']))

                    conf['workload_src'] = WLRUNNER
                    conf['filesystem'] = fs
                    conf['flash_npage_per_block'] = 32
                    if N == 'nflashblocks':
                        N = conf['flash_num_blocks']
                    conf['nkftl']['n_blocks_in_data_group'] = N
                    conf['nkftl']['max_blocks_in_log_group'] = K
                    conf['nkftl']['GC_threshold_ratio'] = 0.8
                    conf['nkftl']['GC_low_threshold_ratio'] = 0.7
                    conf['nkftl']['provision_ratio'] = 1.28

                    conf["workload_class"] = "Synthetic"
                    conf["Synthetic"] = {
                            # "generating_func": "self.generate_hotcold_workload",
                            # "generating_func": "self.generate_sequential_workload",
                            # "generating_func": "self.generate_backward_workload",
                            "generating_func": "self.generate_random_workload",
                            # "chunk_count": 100*2**20/(8*1024),
                            "chunk_count": filesize / chunksize,
                            "chunk_size" : chunksize,
                            "iterations" : int(bytes_to_write/filesize),
                            "filename"   : "test.file",
                            "n_col"      : 5   # only for hotcold workload
                        }

                    runtime_update(conf)

                    print 'filesize', filesize/2**20
                    print conf['Synthetic']['chunk_count'] * conf['Synthetic']['chunk_size'] / 2**20
                    print 'iteration', conf['Synthetic']['iterations']

                    assert loop_dev_mb > conf["Synthetic"]["chunk_count"] \
                        * conf["Synthetic"]["chunk_size"] / 2**20
                    workflow(conf)

def choose_exp_metadata(default_conf):
    """
    This function will return a dictionary containing a few things relating
    to result dir. You can update the experiment configuration by
    confdic.update(return of this function)

    default_conf is readonly in this function.

    Usage: Call this function for once to get a dictionary with things needing
    to be updated. Use the returned dictionary multiple times to update
    experimental config. Note that conf['result_dir'] still needs to be updated
    later for each experiment.
    """
    conf = {}
    toresult = raw_input('Save this experiments to /tmp/results? (y/n)')
    if toresult.lower() == 'y':
        targetdir = '/tmp/results'
        expname = raw_input('Enter expname ({}):'.format(default_conf['expname']))
        if expname.strip() != '':
            conf['expname'] = expname
        else:
            conf['expname'] = default_conf['expname']

        subexpname = raw_input('Enter subexpname ({}):'.format(
            default_conf['subexpname']))
        if subexpname.strip() != '':
            conf['subexpname'] = subexpname
        else:
            conf['subexpname'] = default_conf['subexpname']
    else:
        targetdir = '/tmp/resulttmp'
        conf['expname'] = default_conf['expname']
        conf['subexpname'] = default_conf['subexpname']

    conf['targetdir'] = targetdir
    return conf

def runtime_update(conf):
    """
    This function has to be called before running each treatment.
    """
    conf['time'] = time.strftime("%m-%d-%H-%M-%S", time.localtime())
    conf['hash'] = hash(str(conf))
    if conf.has_key('filesystem') and conf['filesystem'] != None:
        fs = str(conf['filesystem'])
    else:
        fs = 'fsnotset'
    conf['result_dir'] = "{targetdir}/{expname}/{subexpname}-{unique}".format(
        targetdir = conf['targetdir'], expname = conf['expname'],
        subexpname = conf['subexpname'],
        unique = '-'.join((fs, conf['time'], str(conf['hash']))))

def test_dftl2_new():
    confdic = get_default_config()
    conf = config.Config(confdic)
    conf['ftl_type'] = 'dftl2'

    metadata_dic = choose_exp_metadata(conf)
    conf.update(metadata_dic)

    def run(fs, nfiles, divider, filesize, chunksize):
        conf["age_workload_class"] = "NoOp"

        devsize_mb = 1024 / divider
        entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])
        conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        conf['interface_level'] =  'page'
        conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))
        conf['loop_dev_size_mb'] = devsize_mb
        conf['filesystem'] =  fs

        # Setup workload
        # filesize = 256 + 128) * MB
        # chunksize = 16 * KB
        bytes_to_write = filesize * 8
        conf["workload_class"] = "Synthetic"
        conf["workload_conf"] = {
                # "generating_func": "self.generate_hotcold_workload",
                # "generating_func": "self.generate_sequential_workload",
                # "generating_func": "self.generate_backward_workload",
                # "generating_func": "self.generate_random_workload",
                "generating_func": "self.generate_parallel_random_writes",
                # "generating_func": gen_func,
                # "chunk_count": 100*2**20/(8*1024),
                "chunk_count": filesize / chunksize,
                "chunk_size" : chunksize,
                "iterations" : int(bytes_to_write/filesize),
                "filename"   : "test.file",
                "n_col"      : 5,   # only for hotcold workload
                "nfiles"     : nfiles   # for parallel random writes
            }

        runtime_update(conf)

        workflow(conf)

    for fs in ('ext4', 'f2fs'):
        for nfiles in (2, 1):
            for filesize in (128 * MB, 128 * 3 * MB):
                for chunksize in (16 * KB, 512 * KB):
                    if nfiles == 1:
                        run(fs = fs, nfiles = nfiles, divider = 2,
                            filesize = filesize, chunksize = chunksize)
                    else:
                        run(fs = fs, nfiles = nfiles, divider = 1,
                            filesize = filesize, chunksize = chunksize)


def test_dftl2_new_parallel_write():
    confdic = get_default_config()
    conf = config.Config(confdic)
    conf['ftl_type'] = 'dftl2'
    conf['enable_e2e_test'] = True

    metadata_dic = choose_exp_metadata(conf)
    conf.update(metadata_dic)

    def run(fs, divider, syn_conf):
        conf["age_workload_class"] = "NoOp"

        devsize_mb = 1024 / divider
        entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])
        conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        conf['interface_level'] =  'page'
        conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))
        conf['loop_dev_size_mb'] = devsize_mb
        conf['filesystem'] =  fs

        # Setup workload
        conf["workload_class"] = "WlMultiWriters"
        conf["workload_conf"] = {
                "generating_func": "self.generate_parallel_writes",
                "filename"   : "test.file",
            }
        conf["workload_conf"].update(syn_conf)
        print conf['workload_conf']

        runtime_update(conf)

        workflow(conf)

    FILESIZE = 256 * MB
    NWRITES = int(1 * GB / (64 * KB))
    confs = [
                # single sequential
                {
                    "name"       : 'single-seq',
                    "filesizes"  : [FILESIZE],
                    "patterns"   : ['sequential'],
                    "write_sizes": [64 * KB],
                    "n_writes": [NWRITES]
                },
                # seq + random
                {
                    "name"       : 'seq+rand',
                    "filesizes"  : [FILESIZE, FILESIZE],
                    "patterns"   : ['sequential', 'random'],
                    "write_sizes": [64 * KB, 64 * KB],
                    "n_writes": [NWRITES, NWRITES]
                },
                # mixing sequential
                {
                    "name"       : 'mix-seq',
                    "filesizes"  : [FILESIZE, FILESIZE],
                    "patterns"   : ['sequential', 'sequential'],
                    "write_sizes": [64 * KB, 64 * KB],
                    "n_writes": [NWRITES, NWRITES]
                },
                # mixing random
                {
                    "name"       : 'mix-rand',
                    "filesizes"  : [FILESIZE, FILESIZE],
                    "patterns"   : ['random', 'random'],
                    "write_sizes": [64 * KB, 64 * KB],
                    "n_writes": [NWRITES, NWRITES]
                },
                # single random
                {
                    "name"       : 'single-rand',
                    "filesizes"  : [FILESIZE],
                    "patterns"   : ['random'],
                    "write_sizes": [64 * KB],
                    "n_writes": [NWRITES]
                }
            ]

    new_confs = []
    for _ in range(3):
        for fs in ('f2fs', 'ext4'):
            for conf_update in confs:
                c = copy.deepcopy(conf_update)
                c['fs'] = fs
                new_confs.append(c)

    random.shuffle(new_confs)

    for conf_update in new_confs:
        nfiles = len(conf_update['filesizes'])
        print conf_update
        fs = conf_update['fs']
        if nfiles == 1:
            run(fs = fs, divider = 2, syn_conf = conf_update)
        else:
            run(fs = fs, divider = 1, syn_conf = conf_update)

def smallnlarge():
    """
    sequential
        - full associative
            - page-level:  small=large
            - hybrid:      small is better than large
        - sast
            - page-level:  large is better than small
            - hybrid:      large is better than small
    random
        - full associative
            - page-level:  small=large (space is enough)
            - hybrid:      small=large (space is enough)
        - sast
            - page-level:  large is better than small (no need to merge)
            - hybrid:      large is better than small (no need to merge)
    """

    GB = 2**30
    MB = 2**20
    KB = 2**10

    TRAFFIC = 1 * GB
    WSIZE = 64 * KB
    allresults = []

    conf_table = []
    for _ in range(2):
        for filesize in [256 * MB]:
            for pattern in ["random", "sequential"]:
                para = [
                  { 'file_size': filesize,
                    'write_size': WSIZE,
                    'n_writes': TRAFFIC / WSIZE,
                    'pattern': pattern,
                    'fsync': 1,
                    'sync': 0,
                    'file_path': '/dev/sdc1',
                    'tag': 'mytag'
                  }
                ]
                conf_table.append(para)

    random.shuffle(conf_table)

    for para in conf_table:
        mw = WlRunner.multiwriters.MultiWriters("../wlgen/player-runtime",
                para)
        # use this to discard device
        shcmd("sudo mkfs.f2fs /dev/sdc1")
        results = mw.run()
        allresults.extend(results)

    table_to_file(allresults, "/tmp/smallnlarge.results.txt")


def get_dev_by_hostname():
    if 'susitna' in socket.gethostname():
        return "/dev/sda1" # or sth. like /dev/sdc1
    else:
        return "/dev/sdc1" # or sth. like /dev/sdc1

def disable_ext4_journal(conf):
    if '^has_journal' in conf['ext4']['make_opts']['-O']:
        conf['ext4']['make_opts']['-O'].remove('^has_journal')

    if 'has_journal' in conf['ext4']['make_opts']['-O']:
        conf['ext4']['make_opts']['-O'].remove('has_journal')

    conf['ext4']['make_opts']['-O'].append('^has_journal')

    try:
        del conf['mnt_opts']['ext4']['data']
    except KeyError:
        pass

def test_fio():
    confdic = get_default_config()
    conf = config.Config(confdic)

    metadata_dic = choose_exp_metadata(conf)
    conf.update(metadata_dic)

    parameters = testfio.build_patterns()

    conf['use_fs'] = True
    conf["n_online_cpus"] = 16

    for para in parameters:
        job_desc = testfio.build_one_run(pattern_tuple = para['pattern'],
                bs = para['bs'], usefs = conf['use_fs'], conf = conf,
                traffic_size = para['traffic_size'],
                file_size = para['file_size'],
                fdatasync = para['fdatasync'],
                bssplit = para['bssplit']
                )
        conf['workload_conf'] = job_desc
        conf['workload_conf_key'] = 'workload_conf'
        conf['fio_para'] = para
        conf['device_path'] = get_dev_by_hostname()
        conf['device_type'] = "real" # loop, rea'

        # perf
        conf['wrap_by_perf'] = True
        conf['perf']['perf_path'] = '/mnt/sdb1/linux-4.1.5/tools/perf/perf'
        conf['perf']['flamegraph_dir'] = '/users/jhe/flamegraph'

        if conf['use_fs']:
            conf['workload_class'] = 'FIO'
            conf['filesystem'] = para['fs']

            if para['fs'] == 'ext4':
                # disable journal
                disable_ext4_journal(conf)

            conf['loop_dev_size_mb'] = para['dev_mb']
            runtime_update(conf)
            workflow(conf)
        else:
            runtime_update(conf)
            fio = WlRunner.workload.FIO(conf, 'workload_conf')
            save_conf(conf)

            print '-------------------------'
            print str(job_desc)
            print '========================='

            fio.run()


def main(cmd_args):
    if cmd_args.git == True:
        shcmd("sudo -u jun git commit -am 'commit by Makefile: {commitmsg}'"\
            .format(commitmsg=cmd_args.commitmsg \
            if cmd_args.commitmsg != None else ''), ignore_error=True)
        shcmd("sudo -u jun git pull")
        shcmd("sudo -u jun git push")

    #function you want to call
    # parse_blkparse('./bigsample', 'myresult')
    # shcmd("scp jun@192.168.56.102:/tmp/ftlsim.in ./FtlSim/misc/")

    # shcmd("git pull && git commit -am 'commit by Makefile'")
    # pure_sequential_or_random()
    # from_filesystem()
    # seq_with_rand_start()
    # pass
    # mdtest_on_filesystems()
    # tpcc_on_filesystems()
    # sqlbench_on_filesystems()
    # synthetic_on_filesystems()
    # test_ftl()
    # test_experimental_design()

    # test_nkftl()
    # test_dftl2_new_parallel_write()
    test_fio()

def _main():
    parser = argparse.ArgumentParser(
        description="This file hold command stream." \
        'Example: python Makefile.py doexp1 '
        )
    parser.add_argument('-t', '--target', action='store')
    parser.add_argument('-c', '--commitmsg', action='store')
    parser.add_argument('-g', '--git',  action='store_true',
        help='snapshot the code by git')
    args = parser.parse_args()

    if args.target == None:
        main(args)
    else:
        # WARNING! Using argument will make it less reproducible
        # because you have to remember what argument you used!
        targets = args.target.split(';')
        for target in targets:
            eval(target)
            # profile.run(target)

if __name__ == '__main__':
    _main()





