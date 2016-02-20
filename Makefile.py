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
import simpy

import config
from config import WLRUNNER, LBAGENERATOR
from environments import *
import experiment
import testfio
import FtlSim
import WlRunner
from utils import *

class Experiment(object):
    def __init__(self):
        # Get default setting
        self.conf = config.Config()

    def setup_environment(self):
        raise NotImplementedError

    def setup_workload(self):
        raise NotImplementedError

    def setup_ftl(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def main(self):
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.run()


def save_conf(conf):
    confpath = os.path.join(conf['result_dir'], 'config.json')
    prepare_dir_for_path(confpath)
    conf.dump_to_file(confpath)


def workflow(conf):
    # save config file for reference
    save_conf(conf)
    event_iter = run_workload(conf)
    run_simulator(conf, event_iter)

def run_workload(conf):
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

    return event_iter

def run_simulator(conf, event_iter):
    if not conf['enable_blktrace'] or not conf['enable_simulation']:
        return

    simulator = create_simulator( conf['simulator_class'], conf, event_iter )
    simulator.run()

def create_simulator(simulator_class, conf, event_iter):

    """
    type: "DES", "NonDES"
    """
    return eval("FtlSim.simulator.{sim_class}(conf, event_iter)".format(
        sim_class = simulator_class))

    # if simulator_type == 'DES':
        # return FtlSim.simulator.SimulatorDES(conf, event_iter)
    # elif simulator_type == 'NonDES':
        # return FtlSim.simulator.SimulatorNonDES(conf, event_iter)
    # else:
        # raise NotImplementedError()

def run_non_des_simulator(conf, event_iter):
    # run the Ftl Simulator
    print "Start simulation.........."
    sim = FtlSim.simulator.Simulator(conf)
    sim.run(event_iter)

def run_des_simulator(conf):
    sim = FtlSim.simulator.SimulatorDES(conf)

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
    conf['simulation_processor'] = 'e2e'
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
    conf['simulation_processor'] = 'e2e'

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

def set_exp_metadata(conf, save_data, expname, subexpname):
    if save_data == True:
        targetdir = '/tmp/results'
    else:
        targetdir = '/tmp/tmpresults'

    conf['targetdir'] = targetdir

    conf['expname'] = expname
    conf['subexpname'] = subexpname


def choose_exp_metadata(default_conf, interactive = True):
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
    result_dir = '/tmp/results'
    # result_dir = '/users/jhe/results'
    # result_dir = '/mnt/ramdisk/results'
    if interactive == True:
        toresult = raw_input('Save this experiments to {}? (y/n)'.format(result_dir))
    else:
        toresult = 'n'
    if toresult.lower() == 'y':
        targetdir = result_dir
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
    conf['simulation_processor'] = 'e2e'
    conf['enable_blktrace'] = True
    conf['enable_simulation'] = False

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

        disable_ext4_journal(conf)

        runtime_update(conf)

        workflow(conf)

    FILESIZE = 256 * MB
    NWRITES = int(256*MB / (64*KB))
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

    confs = [
                # mixing random
                {
                    "name"       : 'mix-rand',
                    "filesizes"  : [FILESIZE, FILESIZE],
                    "patterns"   : ['random', 'random'],
                    # "write_sizes": [32 * KB, 32 * KB],
                    "write_sizes": [32 * KB, 32 * KB],
                    "n_writes": [FILESIZE/(32*KB), FILESIZE/(32*KB)]
                }
            ]

    new_confs = []
    for _ in range(1):
        for fs in ['ext4']:
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

def enable_ext4_journal(conf):
    if '^has_journal' in conf['ext4']['make_opts']['-O']:
        conf['ext4']['make_opts']['-O'].remove('^has_journal')

    if 'has_journal' in conf['ext4']['make_opts']['-O']:
        conf['ext4']['make_opts']['-O'].remove('has_journal')

    conf['ext4']['make_opts']['-O'].append('has_journal')

    try:
        del conf['mnt_opts']['ext4']['data']
    except KeyError:
        pass


def test_fio():
    conf = config.ConfigNewFlash()

    metadata_dic = choose_exp_metadata(conf)
    conf.update(metadata_dic)

    parameters = testfio.build_patterns()

    conf['use_fs'] = True
    conf["n_online_cpus"] = 1

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
        # conf['device_path'] = get_dev_by_hostname()
        conf['device_path'] = "/dev/loop0"
        conf['device_type'] = "loop" # loop, rea'

        conf['simulator_class'] = 'SimulatorNonDESe2e'
        conf['ftl_type'] = "dftlext"

        conf['flash_config']['page_size'] = 1024
        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / conf.page_size)
        conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

        # perf
        conf['wrap_by_perf'] = False
        conf['perf']['perf_path'] = '/mnt/sdb1/linux-4.1.5/tools/perf/perf'
        conf['perf']['flamegraph_dir'] = '/users/jhe/flamegraph'

        conf['enable_blktrace'] = True
        conf['enable_simulation'] = True

        MOpt = WlRunner.filesystem.MountOption
        conf["mnt_opts"]["ext4"]['discard'] = MOpt(opt_name = "discard",
                                         value = "nodiscard",
                                         include_name = False)

        if conf['use_fs']:
            conf['workload_class'] = 'FIO'
            conf['filesystem'] = para['fs']

            if para['fs'] == 'ext4':
                disable_ext4_journal(conf)
                # enable_ext4_journal(conf)

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

def reproduce_slowness():
    try:
        shcmd("umount /dev/sdc1")
    except RuntimeError:
        pass
    shcmd("mkfs.ext4 -O ^has_journal /dev/sdc1")
    #shcmd("mount -o discard /dev/sdc1 /mnt/fsonloop")
    print 'fs made....'
    time.sleep(2)

    shcmd("mount -o nodiscard /dev/sdc1 /mnt/fsonloop")

    print 'mounted.........'
    time.sleep(1)

    # shcmd("echo 0 > /sys/block/sdc/queue/discard_zeroes_data")
    # shcmd("fio ./reproduce.ini")


def reproduce_slowness_with_blktrace():
    def prepare():
        try:
            shcmd("umount /dev/sdc1")
        except RuntimeError:
            pass
        shcmd("mkfs.ext4 -O ^has_journal /dev/sdc1")
        #shcmd("mount -o discard /dev/sdc1 /mnt/fsonloop")
        print 'fs made....'
        time.sleep(2)

        shcmd("mount -o nodiscard /dev/sdc1 /mnt/fsonloop")

        print 'mounted.........'
        time.sleep(1)

    def test_func_run():
        shcmd("fio ./reproduce.ini")

    def btt(dir_path, devname):
        with cd(dir_path):
            shcmd("blkrawverify {}".format(devname))
            shcmd("cat {}.verify.out".format(devname))
            shcmd("blkparse {devname} -d bp.{devname}.bin > /dev/null".format(
                devname = devname))
            shcmd("btt -A -i bp.{devname}.bin > bp.{devname}.txt"\
                    .format(devname = devname))


    # btt("/tmp/blk-recusing-existing", 'sdc1')
    # return

    prepare()
    return

    shcmd("strace fio ./reproduce.ini &> strace.log")
    return

    # shcmd("fio ./reproduce.ini")
    suf = 'prepare002-withjournal3'
    # suf = 'using-existing'
    record_dir = '/tmp/blk-rec' + suf
    replay_dir = '/tmp/blk-replay' + suf
    fio_replay_dir = '/tmp/fio' + suf

    wrapper = WlRunner.traceandreplay.BlktraceWrapper(
            "/dev/sdc1", "", record_dir, "BlktraceRunnerAlone")
    wrapper.wrapped_run(test_func_run)

    btt(record_dir, 'sdc1')


class DftlextExp001(Experiment):
    def __init__(self):
        # Get default setting
        self.conf = FtlSim.dftlext.Config()
        self.devsize_mb = 128

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = True)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

        self.conf['filesystem'] = 'ext4'
        self.conf['loop_dev_size_mb'] = self.devsize_mb

        self.conf['device_path'] = "/dev/loop0"
        self.conf['device_type'] = "loop" # loop, rea'

        self.conf['flash_config']['page_size'] = 1024

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        filesize = 32*MB
        job_desc = testfio.build_one_run(pattern_tuple = ['randwrite'],
                bs = 32*KB, usefs = True, conf = self.conf,
                traffic_size = 32*MB,
                file_size = filesize,
                fdatasync = 1,
                bssplit = WlRunner.fio.HIDE_ATTR
                )
        assert filesize < self.devsize_mb * MB

        self.conf['workload_conf'] = job_desc
        self.conf['workload_conf_key'] = 'workload_conf'
        # self.conf['fio_para'] = para

        self.conf["workload_src"] = WLRUNNER
        self.conf["workload_class"] = "FIO"
        self.conf["age_workload_class"] = "NoOp"


        self.conf['wrap_by_perf'] = False

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        entries_need = int(self.devsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf.max_cmt_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(self.devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

def DftlextExp001_run():
    obj = DftlextExp001()
    obj.main()


class FIO_DFTLDES(object):
    def __init__(self):
        # Get default setting
        self.conf = FtlSim.dftldes.Config()
        self.devsize_mb = 256

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = True)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

        self.conf['filesystem'] = 'ext4'
        self.conf['loop_dev_size_mb'] = self.devsize_mb

        self.conf['device_path'] = "/dev/loop0"
        self.conf['device_type'] = "loop" # loop, rea'

        self.conf['flash_config']['page_size'] = 1024

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        filesize = 64*MB
        job_desc = testfio.build_one_run(pattern_tuple = ['randwrite'],
                bs = 32*KB, usefs = True, conf = self.conf,
                traffic_size = 64*MB,
                file_size = filesize,
                fdatasync = 1,
                bssplit = WlRunner.fio.HIDE_ATTR
                )
        assert filesize < self.devsize_mb * MB

        self.conf['workload_conf'] = job_desc
        self.conf['workload_conf_key'] = 'workload_conf'
        # self.conf['fio_para'] = para

        self.conf["workload_src"] = WLRUNNER
        self.conf["workload_class"] = "FIO"
        self.conf["age_workload_class"] = "NoOp"


        self.conf['wrap_by_perf'] = False

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        self.conf['flash_config']['n_channels_per_dev'] = 8
        self.conf['SSDFramework']['ncq_depth'] = 4

        entries_need = int(self.devsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf.max_cmt_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(self.devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def main(self):
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.run()

def run_FIO_DFTLDES():
    obj = FIO_DFTLDES()
    obj.main()


class NCQExp(object):
    def __init__(self, ncq_depth, n_channels, trafficsize, expname, mode):
        self.ncq_depth = ncq_depth
        self.n_channels = n_channels
        self.trafficsize = trafficsize
        self.expname = expname
        self.mode = mode

    def setup_config(self):
        self.conf = FtlSim.dftldes.Config()
        self.conf['SSDFramework']['ncq_depth'] = self.ncq_depth

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = 64
        self.conf['flash_config']['n_blocks_per_plane'] = 32
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = self.n_channels

    def setup_environment(self):
        subname_items = [self.ncq_depth, self.n_channels, self.trafficsize]
        subname_items = [str(x) for x in subname_items]
        set_exp_metadata(self.conf, save_data = True,
                expname = self.expname,
                subexpname = '.'.join(subname_items))

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "TestWorkloadFLEX3"

        chunk_size = 32*KB
        page_size = self.conf['flash_config']['page_size']
        self.conf["lba_workload_configs"]["TestWorkloadFLEX3"] = {
                "op_count": self.trafficsize/chunk_size,
                "extent_size": chunk_size/page_size ,
                "ops": ['write'], 'mode': self.mode}
                # "ops": ['read', 'write', 'discard']}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 256
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.max_cmt_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 2))
        print "Current n_blocks_per_plane",\
            self.conf['flash_config']['n_blocks_per_plane']

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

def run_ncqexp():
    expname = raw_input("HELLO, you expname please:")
    if expname == '':
        expname = 'default-expname'

    for ncq_depth in (1, 16, 32):
        for n_channels in (32, 256):
            for mode in ("random", "sequential"):
                exp = NCQExp(
                        ncq_depth = ncq_depth,
                        n_channels = n_channels,
                        trafficsize = 64*MB,
                        expname = expname,
                        mode = mode)
                exp.test_main()

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
    # test_fio()
    # DftlextExp001_run()
    run_FIO_DFTLDES()

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





