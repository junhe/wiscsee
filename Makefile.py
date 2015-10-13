#!/usr/bin/env python
import itertools
import random
import argparse
import re
import subprocess
import os
import profile
import sys
import shlex
import time
import glob
import pprint
from time import localtime, strftime

import config
from config import WLRUNNER, LBAGENERATOR
import experiment
import FtlSim
import WlRunner
from utils import *


#########################################################
# Git helper
# you can use to get hash of the code, which you can put
# to your results
def git_latest_hash():
    cmd = ['git', 'log', '--pretty=format:"%h"', '-n', '1']
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE)
    proc.wait()
    hash = proc.communicate()[0]
    hash = hash.strip('"')
    print hash
    return hash

def git_commit(msg='auto commit'):
    shcmd('git commit -am "{msg}"'.format(msg=msg),
            ignore_error=True)

def workflow(conf):
    # save config file for reference
    confpath = os.path.join(conf['result_dir'], 'config.json')
    prepare_dir_for_path(confpath)
    conf.dump_to_file(confpath)

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

    # run the Ftl Simulator
    print "Start simulation.........."
    sim = FtlSim.simulator.Simulator(conf)
    sim.run(event_iter)


def seq_with_rand_start():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : "/tmp/simple",
        "workload_src" : LBAGENERATOR,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 16,
        "flash_num_blocks"      : 64*2**20/(4096*16),

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "log_block_upperbound_ratio" : 0.5,

        "verbose_level" : 1,
        "comment1"      : "output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : 8,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",


        "sector_size"           : 512,

        "filesystem"            : "ext4",

        "workload_class"        : "Simple",
        "lba_workload_class"    : "SeqWithRandomStart", # in WlRunner/lbaworkloadgenerator.py
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 0.5    #how many writes you want to have
        }
    }

    conf = config.Config(confdic)

    # ftls = ("blockmap", "pagemap", "hybridmap")
    ftls = ("hybridmap",)
    for ftl in ftls:
        conf['result_dir'] = os.path.join('/tmp/seq_randstart', ftl)
        conf['ftl_type'] = ftl
        workflow(conf)

def from_filesystem():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : "/tmp/simple",
        "workload_src" : WLRUNNER,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 16,
        "flash_num_blocks"      : 64*2**20/(4096*16),

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "log_block_upperbound_ratio" : 0.5,

        "verbose_level" : 1,
        "comment1"      : "output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : 8,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",


        "sector_size"           : 512,

        "filesystem"            : "ext4",

        "workload_class"        : "Simple",
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 2    #how many writes you want to have
        }
    }

    conf = config.Config(confdic)
    for fs in ('ext4', 'f2fs'):
        conf['result_dir'] = "/tmp/simple/"+fs
        workflow(conf)

def mdtest_on_filesystems():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : None,
        "workload_src" : WLRUNNER,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 16,
        "flash_num_blocks"      : None,

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "log_block_upperbound_ratio" : 0.5,

        "verbose_level" : 1,
        "# comment1"      : "output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",


        "sector_size"           : 512,

        "filesystem"            : "ext4",

        "workload_class"        : "Mdtest",
        "mdtest_settings"       : {
            "np" : 1,
            "branches" : 2,
            "items_per_node" : 50,
            "depth" : 7,
            "write_bytes": 4096,
            "sync_write": "", #"-y", # use "" to turn it off
            "create_only": "-C",
            "iterations": 1
        },

        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 2    #how many writes you want to have
        }
    }

    # filesystems = ('ext4',)
    # filesystems = ('f2fs',)
    # filesystems = ('btrfs',)
    filesystems = ('ext4', 'btrfs')
    expname = 'long-mdtest-2'
    for fs in filesystems:
        devsize_mb = 256
        conf = config.Config(confdic)
        conf['filesystem'] = fs
        conf['ftl_type'] = 'hybridmap'
        conf['result_dir'] = "/tmp/{}/".format(expname) + \
            '-'.join([fs, conf['ftl_type'], str(devsize_mb)])
        conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
        conf['loop_dev_size_mb'] = devsize_mb

        workflow(conf)

def tpcc_on_filesystems():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : None,
        "workload_src" : WLRUNNER,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 16,
        "flash_num_blocks"      : None,

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "log_block_upperbound_ratio" : 0.5,

        "verbose_level" : 1,
        "# comment1"      : "output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",


        "sector_size"           : 512,

        "filesystem"            : "ext4",

        "workload_class"        : "Tpcc",

        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 2    #how many writes you want to have
        }
    }

    # filesystems = ('ext4',)
    # filesystems = ('f2fs',)
    # filesystems = ('btrfs',)
    filesystems = ('ext4',)
    expname = 'tpcc'
    for fs in filesystems:
        devsize_mb = 4096
        conf = config.Config(confdic)
        conf['filesystem'] = fs
        conf['ftl_type'] = 'hybridmap'
        conf['result_dir'] = "/tmp/{}/".format(expname) + \
            '-'.join([fs, conf['ftl_type'], str(devsize_mb)])
        conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
        conf['loop_dev_size_mb'] = devsize_mb

        workflow(conf)

def sqlbench_on_filesystems():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : None,
        "workload_src" : WLRUNNER,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 16,
        "flash_num_blocks"      : None,

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "log_block_upperbound_ratio" : 0.5,

        "verbose_level" : 1,
        "# comment1"      : "output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",


        "sector_size"           : 512,

        "filesystem"            : "ext4",

        "workload_class"        : "Sqlbench",
        "sqlbench" :{
            "benches_to_run": ["test-ATIS"]
        },

        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 2    #how many writes you want to have
        }
    }


    strlist = "test-ATIS test-big-tables test-create test-select test-wisconsin "\
              "test-alter-table test-connect test-insert test-transactions"
    sqlbenchlist = strlist.split()
    sqlbenchlist.remove('test-insert')
    sqlbenchlist.remove('test-create')
    # sqlbenchlist = ["test-ATIS"]
    # sqlbenchlist = ["test-create"]

    filesystems = ('ext4',)
    # filesystems = ('f2fs',)
    # filesystems = ('btrfs',)
    # filesystems = ('ext4', 'btrfs', 'f2fs')
    # filesystems = ('ext4', 'btrfs', 'f2fs')
    expname = 'sqlbench-1by1-ext4default'
    for bench in sqlbenchlist:
        for fs in filesystems:
            devsize_mb = 256
            conf = config.Config(confdic)
            conf['filesystem'] = fs
            conf['ftl_type'] = 'hybridmap'
            conf['result_dir'] = "/tmp/{}/".format(expname) + \
                '-'.join([fs, 'ondefault', conf['ftl_type'], str(devsize_mb), bench])
            conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
            conf['loop_dev_size_mb'] = devsize_mb
            conf['sqlbench']['benches_to_run'] = [bench]

            try:
                shcmd("sudo service mysql stop")
            except Exception:
                pass
            workflow(conf)

def synthetic_on_filesystems():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : None,
        "workload_src"          : WLRUNNER,
        "expname"               : "backwards.nojournal",
        "time"                  : None,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 32,
        "flash_num_blocks"      : None,

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",
        # "ftl_type" : "pagemap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "hybridmapftl": {
            "low_log_block_ratio": 0.32
        },

        "verbose_level" : 1,
        # output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",
        "common_mnt_opts"       : ["discard"],
        # "common_mnt_opts"       : ["discard", "nodatacow"],

        "sector_size"           : 512,

        "filesystem"            : None,
        "ext4" : {
            "make_opts": {'-O':'^has_journal'}
        },

        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "Synthetic" :{
            "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            # "generating_func": "self.generate_random_workload",
            "chunk_count": 100*2**20/(8*1024),
            "chunk_size" : 8*1024,
            "iterations" : 3
        },

        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 2    #how many writes you want to have
        }
    }

    # filesystems = ('ext4',)
    # filesystems = ('f2fs', 'btrfs')
    # filesystems = ('btrfs',)
    # filesystems = ('ext4', 'btrfs', 'f2fs')
    filesystems = ('btrfs', 'f2fs')
    for fs in filesystems:
        devsize_mb = 256
        conf = config.Config(confdic)
        conf['filesystem'] = fs
        conf['time'] = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        conf['result_dir'] = "/tmp/{}/".format(conf['expname']) + \
            '-'.join([fs, conf['ftl_type'], str(devsize_mb)])
        conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
        conf['loop_dev_size_mb'] = devsize_mb

        workflow(conf)

def test_bitmap():
    confdic = {
        "####################################### Global": "",
        "result_dir"            : None,
        "workload_src"          : WLRUNNER,
        "expname"               : "backwards.nojournal",
        "time"                  : None,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 32,
        "flash_num_blocks"      : None,

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : "hybridmap",
        # "ftl_type" : "pagemap",

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "hybridmapftl": {
            "low_log_block_ratio": 0.32
        },

        "verbose_level" : 1,
        # output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",
        "common_mnt_opts"       : ["discard"],
        # "common_mnt_opts"       : ["discard", "nodatacow"],

        "sector_size"           : 512,

        "filesystem"            : None,
        "ext4" : {
            "make_opts": {'-O':'^has_journal'}
        },

        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "Synthetic" :{
            "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            # "generating_func": "self.generate_random_workload",
            "chunk_count": 100*2**20/(8*1024),
            "chunk_size" : 8*1024,
            "iterations" : 3
        },

        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 2    #how many writes you want to have
        }
    }

    devsize_mb = 256
    conf = config.Config(confdic)
    conf.set_flash_num_blocks_by_bytes(128*1024)
    conf['loop_dev_size_mb'] = devsize_mb

    bitmap = FtlSim.ftlbuilder.FlashBitmap2(conf)
    bitmap.initialize()
    print bitmap.bitmap
    print bitmap.is_page_valid(0)
    bitmap.validate_page(0)
    print bitmap.bitmap
    print bitmap.is_page_valid(0)
    bitmap.invalidate_page(0)
    print bitmap.bitmap
    print bitmap.is_page_valid(0)
    bitmap.erase_block(0)
    print bitmap.bitmap
    print bitmap.is_page_valid(0)

def pure_sequential_or_random():
    confdic = {
        "####################################### Global": "",
        # "result_dir"            : "/tmp/exp001",
        "workload_src" : LBAGENERATOR,

        "####################################### For FtlSim": "",
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 16,
        "flash_num_blocks"      : 64*2**20/(4096*16),

        "# dummycomment": ["directmap", "blockmap", "pagemap", "hybridmap"],
        "ftl_type" : None,

        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "log_block_upperbound_ratio" : 0.5,

        "verbose_level" : 1,
        "comment1"      : "output_target: file, stdout",
        "output_target" : "file",

        "####################################### For WlRunner": "",
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : 4096,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",


        "sector_size"           : 512,

        # "filesystem"            : "ext4",
        "filesystem"            : "f2fs",

        "workload_class"        : "Simple",
        # "lba_workload_class"    : "Random",
        "lba_workload_class"    : "Sequential",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.6,
            "write_to_lba_ratio"     : 4    #how many writes you want to have
        }
    }

    conf = config.Config(confdic)

    ftls = ("directmap", "blockmap", "pagemap", "hybridmap")
    # ftls = ("pagemap",)
    # ftls = ("directmap",)
    # ftls = ("hybridmap",)
    for ftl in ftls:
        conf['result_dir'] = os.path.join('/tmp/seqlba_improved_dm_pm_SEQ', ftl)
        conf['ftl_type'] = ftl
        workflow(conf)

def mysql_change_data_dir():
    def start_mysql():
        shcmd("sudo service mysql start")

    def stop_mysql():
        """You will get 'no instance found' if no mysql runningn"""
        shcmd("sudo service mysql stop")

    def change_data_dir():
        lines = []
        with open("/etc/mysql/my.cnf", "r") as f:
            for line in f:
                if line.startswith("datadir"):
                    line = "datadir     = /mnt/fsonloop/mysql\n"
                lines.append(line)

        with open("/etc/mysql/my.cnf", "w") as f:
            for line in lines:
                f.write(line)

        lines = []
        with open("/etc/apparmor.d/usr.sbin.mysqld", "r") as f:
            for line in f:
                line = line.replace('/var/lib/mysql', '/mnt/fsonloop/mysql')
                lines.append(line)

        with open("/etc/apparmor.d/usr.sbin.mysqld", "w") as f:
            for line in lines:
                f.write(line)

    def local_main():
        # stop_mysql()

        shcmd("cp -r /var/lib/mysql /mnt/fsonloop/")
        shcmd("chown -R mysql:mysql /mnt/fsonloop/mysql")
        change_data_dir()
        start_mysql()

    local_main()

def test_dftl():
    """
    MEMO:
    - you need to set the high watermark properly. Otherwise it will trigger
    victim selection too often, which has high overhead.
    - also, set low watermark to as low as possible, so we get the most free
    pages out of each cleaning. So we don't need to trigger cleaning so often.
    """
    confdic = {
        ############### Global #########
        "result_dir"            : None,
        # "workload_src"          : WLRUNNER,
        "workload_src"          : LBAGENERATOR,
        "expname"               : "debugdftl2",
        "time"                  : None,
        # directmap", "blockmap", "pagemap", "hybridmap", dftl2
        "ftl_type"              : "dftl2",
        "sector_size"           : 512,

        ############## For FtlSim ######
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 32,
        "flash_num_blocks"      : None,

        ############## Dftl ############
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 4, # 32 bits
            "GC_threshold_ratio": 0.8,
            "GC_low_threshold_ratio": 0.4,
            "max_cmt_bytes": None # cmt: cached mapping table
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
        # "output_target" : "stdout",

        ############## For WlRunner ########
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",
        "common_mnt_opts"       : ["discard"],
        # "common_mnt_opts"       : ["discard", "nodatacow"],
        "filesystem"            : None,


        ############## FS ##################
        "ext4" : {
            "make_opts": {'-O':'has_journal'}
        },

        ############## workload.py on top of FS #########
        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "Synthetic" :{
            "generating_func": "self.generate_hotcold_workload",
            # "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            # "generating_func": "self.generate_random_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": 5,
            "chunk_size" : 4*1024*1024,
            "iterations" : 5,
            "n_col"      : 5   # only for hotcold workload
        },

        ############## LBAGENERATOR  #########
        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Sequential",
        # "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.1,
            "write_to_lba_ratio"     : 1    #how many writes you want to have
        }
    }

    # TODO: USE LARGER DISK
    # filesystems = ('ext4', 'f2fs', 'btrfs')
    # filesystems = ('f2fs', 'btrfs')
    # filesystems = ('f2fs',)
    filesystems = ('ext4',)
    # filesystems = ('ext4', 'btrfs', 'f2fs')
    # filesystems = ('xfs',)
    # filesystems = ('btrfs',)
    # filesystems = ('btrfs','f2fs')
    for fs in filesystems:
        devsize_mb = 256
        conf = config.Config(confdic)
        conf['filesystem'] = fs
        conf['time'] = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())

        # hold 3% flash pages' mapping entries
        entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])
        conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        print "conf['dftl']['max_cmt_bytes']", conf['dftl']['max_cmt_bytes']

        conf['result_dir'] = "/tmp/results/{}/".format(conf['expname']) + \
            '-'.join([fs, conf['ftl_type'], str(devsize_mb), 'cmtsize',
            str(conf['dftl']['max_cmt_bytes'])])
        conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
        conf['loop_dev_size_mb'] = devsize_mb

        workflow(conf)

def test_dftl2():
    """
    MEMO:
    - you need to set the high watermark properly. Otherwise it will trigger
    victim selection too often, which has high overhead.
    - also, set low watermark to as low as possible, so we get the most free
    pages out of each cleaning. So we don't need to trigger cleaning so often.
    """
    confdic = {
        ############### Global #########
        "result_dir"            : None,
        "workload_src"          : WLRUNNER,
        # "workload_src"          : LBAGENERATOR,
        "expname"               : "dftl2",
        "time"                  : None,
        "subexpname"            : "testbatchupdate",
        # directmap", "blockmap", "pagemap", "hybridmap", dftl2
        "ftl_type"              : "dftl2",
        "sector_size"           : 512,

        ############## For FtlSim ######
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 32,
        "flash_num_blocks"      : None,
        # "interface_level"       : "page", # or range
        "interface_level"       : "range",

        ############## Dftl ############
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 4, # 32 bits
            "GC_threshold_ratio": 0.8,
            "GC_low_threshold_ratio": 0.4,
            "max_cmt_bytes": None, # cmt: cached mapping table
            "tpftl": {
                "entry_node_bytes": 6, # page 8, TPFTL paper
                "page_node_bytes": 8   # m_vpn, pointer to entrylist
            }
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
        # "output_target" : "stdout",

        ############## For WlRunner ########
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",
        "common_mnt_opts"       : ["discard"],
        # "common_mnt_opts"       : ["discard", "nodatacow"],
        "filesystem"            : None,


        ############## FS ##################
        "ext4" : {
            "make_opts": {'-O':'has_journal'}
        },

        ############## workload.py on top of FS #########
        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "Synthetic" :{
            "generating_func": "self.generate_hotcold_workload",
            # "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            # "generating_func": "self.generate_random_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": 5,
            "chunk_size" : 2*1024*1024,
            "iterations" : 5,
            "n_col"      : 5   # only for hotcold workload
        },

        ############## LBAGENERATOR  #########
        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "HotCold",
        # "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.05,
            "write_to_lba_ratio"     : 1    #how many writes you want to have
        }
    }

    # TODO: USE LARGER DISK
    # filesystems = ('ext4', 'f2fs', 'btrfs')
    # filesystems = ('f2fs', 'btrfs')
    # filesystems = ('f2fs',)
    filesystems = ('ext4',)
    # filesystems = ('ext4', 'btrfs', 'f2fs')
    # filesystems = ('xfs',)
    # filesystems = ('btrfs',)
    # filesystems = ('btrfs','f2fs')
    exptime = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
    for interface in ('page',):
        for fs in filesystems:
            devsize_mb = 256
            conf = config.Config(confdic)
            conf['filesystem'] = fs
            conf['time'] = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())

            # hold 3% flash pages' mapping entries
            entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])
            conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem

            conf['interface_level'] =  interface
            conf['result_dir'] = "/tmp/results/{}/".format(conf['expname']) + \
                '-'.join([fs, conf['ftl_type'], str(devsize_mb), 'cmtsize',
                str(conf['dftl']['max_cmt_bytes']), conf['interface_level'],
                conf['subexpname'], exptime])
            conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
            conf['loop_dev_size_mb'] = devsize_mb

            print conf['result_dir']
            workflow(conf)
            # FtlSim.tpftl.main(conf)


def test_with_conf():
    """
    MEMO:
    - you need to set the high watermark properly. Otherwise it will trigger
    victim selection too often, which has high overhead.
    - also, set low watermark to as low as possible, so we get the most free
    pages out of each cleaning. So we don't need to trigger cleaning so often.
    """
    confdic = {
        ############### Global #########
        "result_dir"            : None,
        # "workload_src"          : WLRUNNER,
        "workload_src"          : LBAGENERATOR,
        "expname"               : "dftl2",
        "time"                  : None,
        "subexpname"            : "testbatchupdate",
        # directmap", "blockmap", "pagemap", "hybridmap", dftl2
        "ftl_type"              : "dftl2",
        "sector_size"           : 512,

        ############## For FtlSim ######
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 32,
        "flash_num_blocks"      : None,
        # "interface_level"       : "page", # or range
        "interface_level"       : "range",

        ############## Dftl ############
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 4, # 32 bits
            "GC_threshold_ratio": 0.8,
            "GC_low_threshold_ratio": 0.4,
            "max_cmt_bytes": None, # cmt: cached mapping table
            "tpftl": {
                "entry_node_bytes": 6, # page 8, TPFTL paper
                "page_node_bytes": 8   # m_vpn, pointer to entrylist
            }
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
        # "output_target" : "stdout",

        ############## For WlRunner ########
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
        "fs_mount_point"        : "/mnt/fsonloop",
        "common_mnt_opts"       : ["discard"],
        # "common_mnt_opts"       : ["discard", "nodatacow"],
        "filesystem"            : None,


        ############## FS ##################
        "ext4" : {
            "make_opts": {'-O':'has_journal'}
        },

        ############## workload.py on top of FS #########
        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "Synthetic" :{
            "generating_func": "self.generate_hotcold_workload",
            # "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            # "generating_func": "self.generate_random_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": 5,
            "chunk_size" : 4*1024*1024,
            "iterations" : 5,
            "n_col"      : 5   # only for hotcold workload
        },

        ############## LBAGENERATOR  #########
        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        "lba_workload_class"    : "Sequential",
        # "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.05,
            "write_to_lba_ratio"     : 1    #how many writes you want to have
        }
    }

    conf = config.Config(confdic)
    for event in WlRunner.lbaworkloadgenerator.HotCold(conf):
        print event


def start_ftrace():
    clear_ftrace()
    with cd("/sys/kernel/debug/tracing"):
        shcmd("echo function > current_tracer")
        # shcmd("echo function_graph > current_tracer")
        shcmd("echo '*:mod:btrfs' > set_ftrace_filter")
        shcmd("echo 1 > tracing_on")
        send_marker("JUN.trace.start")

def stop_ftrace():
    send_marker("JUN.trace.end")
    with cd("/sys/kernel/debug/tracing"):
        shcmd("echo 0 > tracing_on")

def clear_ftrace():
    with cd("/sys/kernel/debug/tracing"):
        shcmd("echo '' > trace")

def send_marker(msg):
    with cd("/sys/kernel/debug/tracing"):
        shcmd("echo {} > trace_marker".format(msg))


def translate_by_factor_space(factor, frac):
    """
    For example:
        translate_by_factor_space('ext4_flex_bg', 0.001) = 'flex_bg'
    """
    factor_table = {
        ################ ext4 ##################
        # mkfs options
        'ext4_flex_bg'          : ['flex_bg', '^flex_bg'],
        'ext4_big_alloc'        : ['bigalloc', '^bigalloc'],
        'ext4_blocksize'        : [4096],
        'ext4_journal_location' : ['start', 'end'], # start of disk, end of disk
        'ext4_has_journal'      : ['has_journal', '^has_journal'],

        # mount options
        'ext4_journal_mode'     : ['journal', 'ordered', 'writeback'],
        'ext4_delay_alloc'      : ['delalloc', 'nodelalloc'],
        'ext4_min_batch_time'   : [0, 30], # unit: ms
        'ext4_journal_ioprio'   : [0, 3, 7], # larger -> higher priority, 3 is default

        ################ f2fs ##################
        # mount options
        'f2fs_background_gc'       : ['on', 'off'],
        'f2fs_disable_roll_forward': ['disable_roll_forward', None],
        'f2fs_no_heap'             : ['no_heap', None],
        'f2fs_active_logs'         : [2, 6],
        'f2fs_flush_merge'         : ['flush_merge', None],
        'f2fs_nobarrier'           : ['nobarrier', None],
        'f2fs_extent_cache'        : ['extent_cache', None],
        'f2fs_noinline_data'       : ['noinline_data', None],

        # mkfs options
        # -a [0 or 1]  : Split start location of each area for heap-based allocation.
        #   1 is set by default, which performs this.
        'f2fs_split'               : [0, 1],

        # 'f2fs_n_seg_per_sec'       : [1, 2],
        'f2fs_n_seg_per_sec'       : [1],
        # 'f2fs_n_sec_per_zone'      : [1, 2],
        'f2fs_n_sec_per_zone'      : [1],

        # /sys/fs/f2fs/<devname>
        'f2fs_ipu_policy'          : [0, 1, 2, 4, 8, 16], # You should also test different combination of them
        'f2fs_min_fsync_blocks'    : [8, 128],
        'f2fs_min_ipu_util'        : [20, 80],

        ################ fs common ##################
        'discard'          : ['discard', 'nodiscard']
    }

    items = factor_table[factor]
    n = len(items)
    pick_index = int(n * frac)
    return items[pick_index]

def apply_to_conf(factor, value, conf):
    """
    For example, apply_to_conf('ext4_flex_bg', 'flex_bg', conf) will add to
    conf['ext4']['make_opts'] 'flex_bg'

    conf essentially is a dict
    """
    MOpt = WlRunner.filesystem.MountOption

    # Ext4
    if factor in ['ext4_flex_bg', 'ext4_big_alloc', 'ext4_has_journal']:
        conf['ext4']['make_opts'].setdefault('-O', []).append(value)
        return conf

    if factor == 'ext4_blocksize':
        conf['ext4']['make_opts']['-b'] = [value]
        return conf

    if factor == 'ext4_journal_location':
        raise NotImplementedError()

    if factor == 'ext4_journal_mode':
        conf['mnt_opts']['ext4']['data'] = MOpt(opt_name = 'data',
                                                value = value,
                                                include_name = True)
        return conf

    if factor == 'ext4_delay_alloc':
        conf['mnt_opts']['ext4']['delalloc'] = MOpt(opt_name = 'delalloc',
                                                value = value,
                                                include_name = False)
        return conf

    if factor == 'ext4_min_batch_time':
        conf['mnt_opts']['ext4']['min_batch_time'] = MOpt(
                                                opt_name = 'min_batch_time',
                                                value = value,
                                                include_name = True)
        return conf

    if factor == 'ext4_journal_ioprio':
        conf['mnt_opts']['ext4']['journal_ioprio'] = MOpt(
                                                opt_name = 'journal_ioprio',
                                                value = value,
                                                include_name = True)
        return conf

    # common
    if factor == 'discard':
        # set all filesystem to discard
        for fs, opts in conf['mnt_opts'].items():
            opts['discard'] = MOpt( opt_name = 'discard',
                                    value = value,
                                    include_name = False)
        return conf


    # F2FS
    # mount options
    if factor in ('f2fs_background_gc', 'f2fs_active_logs'):
        opt_name = factor[5:]
        conf['mnt_opts']['f2fs'][opt_name] = MOpt(
                                                opt_name = opt_name,
                                                value = value,
                                                include_name = True)
        return conf

    if factor in ('f2fs_disable_roll_forward',
                  'f2fs_no_heap',
                  'f2fs_flush_merge',
                  'f2fs_nobarrier',
                  'f2fs_extent_cache',
                  'f2fs_noinline_data'):
        opt_name = factor[5:]
        conf['mnt_opts']['f2fs'][opt_name] = MOpt(
                                            opt_name = opt_name,
                                            value = value,
                                            include_name = False)
        return conf

    # mkfs options
    if factor == 'f2fs_split':
        conf['f2fs']['make_opts']['-a'] = [value]
        return conf

    if factor == 'f2fs_n_seg_per_sec':
        conf['f2fs']['make_opts']['-s'] = [value]
        return conf

    if factor == 'f2fs_n_sec_per_zone':
        conf['f2fs']['make_opts']['-z'] = [value]
        return conf

    if factor in ('f2fs_ipu_policy', 'f2fs_min_fsync_blocks',
            'f2fs_min_ipu_util'):
        opt_name = factor[5:]
        conf['f2fs'].setdefault('sysfs', {})[opt_name] = value
        return conf

def get_design_table(fs):
    ext4_colnames = ['ext4_flex_bg',         'ext4_big_alloc',
                'ext4_blocksize',       'ext4_delay_alloc',
                # 'ext4_journal_location',
                'ext4_min_batch_time',  'ext4_journal_ioprio',
                'ext4_journal_mode',    'ext4_has_journal' ]

    f2fs_colnames = ['f2fs_background_gc',
                'f2fs_disable_roll_forward',
                'f2fs_no_heap',
                'f2fs_active_logs',
                'f2fs_flush_merge',
                'f2fs_nobarrier',
                'f2fs_extent_cache',
                'f2fs_noinline_data',
                'f2fs_split',
                'f2fs_n_seg_per_sec',
                'f2fs_n_sec_per_zone',
                'f2fs_ipu_policy',
                'f2fs_min_fsync_blocks',
                'f2fs_min_ipu_util'
                ]

    if fs == 'ext4':
        colnames = ext4_colnames
    elif fs == 'f2fs':
        colnames = f2fs_colnames

    ncols = len(colnames)
    filepath = './designs/64.rows.{ncols}.cols.txt'.format(ncols = ncols)
    shown = False
    table = []
    with open(filepath, 'r') as f:
        for line in f:
            items = line.split()
            if shown == False and len(colnames) != len(items):
                print ''.join(['=']*80)
                print "WARNING: number of factors does not match number of "\
                        "columns in design file. Results may be misleading!"
                print ''.join(['=']*80)
                time.sleep(1)
                shown = True
            table.append(dict(zip(colnames, items))) # cut the longer list

    return table

def translate_table_for_human(table):
    """
    Translate 0.xxx to human readable configure
    """
    for row in table:
        for factor, frac in row.items():
            row[factor] = translate_by_factor_space(factor, float(frac))

    return table

def treatment_to_config(treatment, confdic):
    """
    This function produces a config for a treatment.

    treatment is list of factors and their values.
    """
    for factor, value in treatment.items():
        apply_to_conf(factor, value, confdic)

    return confdic

def get_default_config():
    MOpt = WlRunner.filesystem.MountOption

    confdic = {
        ############### Global #########
        "result_dir"            : None,
        # "workload_src"          : WLRUNNER,
        "workload_src"          : LBAGENERATOR,
        "expname"               : "default-expname",
        "time"                  : None,
        "subexpname"            : "default-subexp",
        # directmap, blockmap, pagemap, hybridmap, dftl2, tpftl, nkftl
        "ftl_type"              : "nkftl",
        "sector_size"           : 512,

        ############## For FtlSim ######
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 4,
        "flash_num_blocks"      : None,
        "enable_e2e_test"       : True,

        ############## Dftl ############
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 4, # 32 bits
            "GC_threshold_ratio": 0.95,
            "GC_low_threshold_ratio": 0.9,
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
        "loop_path"             : "/dev/loop0",
        "loop_dev_size_mb"      : None,
        "tmpfs_mount_point"     : "/mnt/tmpfs",
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

        ############## workload.py on top of FS #########
        # "workload_class"        : "Simple",
        "workload_class"        : "Synthetic",
        "Synthetic" :{
            # "generating_func": "self.generate_hotcold_workload",
            # "generating_func": "self.generate_sequential_workload",
            # "generating_func": "self.generate_backward_workload",
            "generating_func": "self.generate_random_workload",
            # "chunk_count": 100*2**20/(8*1024),
            "chunk_count": 4 * 2**20 / (512 * 1024),
            "chunk_size" : 512 * 1024,
            "iterations" : 1,
            "n_col"      : 5   # only for hotcold workload
        },

        ############## LBAGENERATOR  #########
        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        # "lba_workload_class"    : "Sequential",
        # "lba_workload_class"    : "HotCold",
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.05,
            "write_to_lba_ratio"     : 1,    #how many writes you want to have
            "HotCold": {
                'chunk_bytes': 4096 * 1024,
                'chunk_count': 4,
                'n_col'      : 4
            }
        },

        ############# OS #####################
        "linux_version": linux_kernel_version()
    }
    return confdic

def test_experimental_design():
    """
    TODO: The configuration is messy..
    """

    progress = 1
    default_conf = get_default_config()
    metadata_dic = choose_exp_metadata(default_conf)
    for fs in ('ext4','f2fs'):
        design_table = get_design_table(fs)
        design_table = translate_table_for_human(design_table)

        for treatment in design_table[0:]:
            # create conf object
            confdic = get_default_config()
            conf = config.Config(confdic)

            # apply treatment to confdic
            treatment_to_config(treatment, confdic)

            # Put treatment in conf for record
            conf['treatment'] = treatment

            # Setup general parameters, such as disk size, ftl cache size
            conf['record_bad_victim_block'] = True
            conf['filesystem'] = fs
            conf['loop_dev_size_mb'] = 1024
            conf.set_flash_num_blocks_by_bytes(
                int(conf['loop_dev_size_mb'] * 2**20 * 1.5) )
            entries_need = int( conf['loop_dev_size_mb'] * 2**20 * 0.03 \
                / conf['flash_page_size'])
            conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem

            # setup workload
            conf["Synthetic"] = {
                    # "generating_func": "self.generate_hotcold_workload",
                    # "generating_func": "self.generate_sequential_workload",
                    # "generating_func": "self.generate_backward_workload",
                    "generating_func": "self.generate_random_workload",
                    "chunk_count": 64 * 2**20 / (32 * 1024),
                    "chunk_size" : 32 * 1024,
                    "iterations" : 50,
                    "n_col"      : 5   # only for hotcold workload
                }

            # update expname and subexpname
            conf.update(metadata_dic)

            # create hash, result dir path, get time
            runtime_update(conf)
            if has_conflicts(conf):
                prepare_dir(conf['result_dir'])
                with open(os.path.join(conf['targetdir'], conf['expname'],
                    'conflicts.txt'), 'a') as f:
                    f.write(str(conf['treatment']) + '\n')
                print 'skip', conf['treatment']
                continue

            workflow(conf)
            print '------------just finished', progress, '-----------------'
            progress += 1

    # Aggregate results
    exp_dir = os.path.join(metadata_dic['targetdir'], metadata_dic['expname'])
    experiment.create_result_table(exp_dir)
    print 'result table created'

def reproduce():
    conf = config.Config()
    conf.load_from_json_file('./5732845.json')
    metadata_dic = choose_exp_metadata(conf)
    conf.update(metadata_dic)
    runtime_update(conf)
    pprint.pprint(conf)
    workflow(conf)

def simple_lba_test():
    confdic = get_default_config()
    conf = config.Config(confdic)
    metadata_dic = {}
    metadata_dic['targetdir'] = '/tmp/results'
    metadata_dic['filesystem'] = 'none'
    metadata_dic['loop_dev_size_mb'] = 16
    conf.update(metadata_dic)

    conf.set_flash_num_blocks_by_bytes(16*2**20)
    runtime_update(conf)
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
    conf['result_dir'] = "{targetdir}/{expname}/{subexpname}-{unique}".format(
        targetdir = conf['targetdir'], expname = conf['expname'],
        subexpname = conf['subexpname'],
        unique = '-'.join((conf['filesystem'], conf['time'], str(conf['hash']))))

def has_conflicts(conf):
    """
    It handles by resetting conf, or it returns True if the conflicts is
    unresolvable.
    """
    try:
        if '^has_journal' in conf['ext4']['make_opts']['-O']:
            # You can think of this resetting as when ^has_journal, setting
            # data=xxx is not effective.
            conf['mnt_opts']['ext4']['data']['value'] = None
            print 'CONFLICT: has no journal but try to set journal mode', \
                    'handled by removing data=xxx'
            return False
    except KeyError:
        return True

    try:
        if conf['mnt_opts']['ext4']['delalloc']['value'] == 'delalloc' and \
            conf['mnt_opts']['ext4']['data']['value'] == 'journal':
            print 'CONFLICT: delalloc with data=journal not supported'
            return True
    except KeyError:
        # configuration not defined, so no way to be conflicted
        return False

def test_ftl():
    """
    MEMO:
    - you need to set the high watermark properly. Otherwise it will trigger
    victim selection too often, which has high overhead.
    - also, set low watermark to as low as possible, so we get the most free
    pages out of each cleaning. So we don't need to trigger cleaning so often.
    """
    confdic = get_default_config()
    apply_to_conf('ext4_flex_bg', '^flex_bg', confdic)
    apply_to_conf('ext4_big_alloc', 'bigalloc', confdic)
    apply_to_conf('ext4_blocksize', 4096, confdic)
    apply_to_conf('ext4_delay_alloc', 'delalloc', confdic)
    apply_to_conf('ext4_min_batch_time', 50, confdic)
    apply_to_conf('ext4_journal_ioprio', 4, confdic)


    # filesystems = ('ext4', 'f2fs', 'btrfs')
    # filesystems = ('f2fs', 'btrfs')
    # filesystems = ('f2fs',)
    # filesystems = ('ext4', 'f2fs')
    filesystems = ('ext4',)
    # filesystems = ('ext4', 'btrfs', 'f2fs')
    # filesystems = ('xfs',)
    # filesystems = ('btrfs',)
    # filesystems = ('btrfs','f2fs')

    toresult = raw_input('Save this experiments to /tmp/results? (y/n)')
    if toresult.lower() == 'y':
        targetdir = '/tmp/results'
        expname = raw_input('Enter expname ({}):'.format(confdic['expname']))
        if expname.strip() != '':
            confdic['expname'] = expname

        subexpname = raw_input('Enter subexpname ({}):'.format(
            confdic['subexpname']))
        if subexpname.strip() != '':
            confdic['subexpname'] = subexpname
    else:
        targetdir = '/tmp/resulttmp'

    exptime = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
    # data_mapped_pool = [8 * 2**20]
    data_mapped_pool = [None]
    for data_mapped in data_mapped_pool:
        for fs in filesystems:
            devsize_mb = 256
            conf = config.Config(confdic)
            conf['filesystem'] = fs
            conf['time'] = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())

            # hold 3% flash pages' mapping entries
            entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])

            # This is the amount of data we want cached mapping
            # data_mapped = 16 * 2**20
            # entries_need =  data_mapped / conf['flash_page_size']
            conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem

            conf['result_dir'] = "{target}/{expname}/".format(
                    target = targetdir, expname = conf['expname']) + \
                '-'.join([conf['subexpname'], exptime, fs, conf['ftl_type'], str(devsize_mb), 'cmtsize',
                str(conf['dftl']['max_cmt_bytes'])])
            conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
            conf['loop_dev_size_mb'] = devsize_mb

            print conf['result_dir']

            # start_ftrace()
            workflow(conf)
            # stop_ftrace()

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
    test_experimental_design()

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

