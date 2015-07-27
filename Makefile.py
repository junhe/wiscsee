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
from time import localtime, strftime

import config
import FtlSim
import WlRunner
from utils import *

WLRUNNER, LBAGENERATOR = ('WLRUNNER', 'LBAGENERATOR')

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
    confdic = {
        ############### Global #########
        "result_dir"            : None,
        "workload_src"          : WLRUNNER,
        # "workload_src"          : LBAGENERATOR,
        "expname"               : "bricks-newtags",
        "time"                  : None,
        # directmap", "blockmap", "pagemap", "hybridmap", dftl
        "ftl_type"              : "dftl",
        "sector_size"           : 512,

        ############## For FtlSim ######
        "flash_page_size"       : 4096,
        "flash_npage_per_block" : 4,
        "flash_num_blocks"      : 128,

        ############## Dftl ############
        "dftl": {
            # number of bytes per entry in global_mapping_table
            "global_mapping_entry_bytes": 32,
            "GC_threshold_ratio": 0.5,
            "GC_low_threshold_ratio": 0.4,
            "max_cmt_bytes": 32*1024 # cmt: cached mapping table
        },

        ############## hybridmap ############
        "high_log_block_ratio"       : 0.4,
        "high_data_block_ratio"      : 0.4,
        "hybridmapftl": {
            "low_log_block_ratio": 0.32
        },

        ############## recorder #############
        "verbose_level" : 1,
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
            "make_opts": {'-O':'^has_journal'}
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
            "chunk_count": 10,
            "chunk_size" : 10*1024*1024,
            "iterations" : 1,
            "n_col"      : 2
        },

        ############## LBAGENERATOR  #########
        # if you choose LBAGENERATOR for workload_src, the following will
        # be used
        # "lba_workload_class"    : "Sequential",
        "lba_workload_class"    : "Random",
        "LBA" : {
            "lba_to_flash_size_ratio": 0.3,
            "write_to_lba_ratio"     : 10    #how many writes you want to have
        }
    }

    # TODO: USE LARGER DISK
    filesystems = ('ext4', 'f2fs', 'btrfs')
    # filesystems = ('f2fs', 'btrfs')
    # filesystems = ('f2fs',)
    # filesystems = ('ext4',)
    # filesystems = ('xfs',)
    for fs in filesystems:
        devsize_mb = 256
        conf = config.Config(confdic)
        conf['filesystem'] = fs
        conf['time'] = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        conf['result_dir'] = "/tmp/results/{}/".format(conf['expname']) + \
            '-'.join([fs, conf['ftl_type'], str(devsize_mb), 'cmtsize',
            str(conf['dftl']['max_cmt_bytes'])])
        conf.set_flash_num_blocks_by_bytes(devsize_mb*2**20)
        conf['loop_dev_size_mb'] = devsize_mb

        workflow(conf)


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
    synthetic_on_filesystems()

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
