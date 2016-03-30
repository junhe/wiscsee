#!/usr/bin/env python
import copy
import collections
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
import string

import config
from config import WLRUNNER, LBAGENERATOR, LBAMULTIPROC
import ssdbox
import workrunner
from utils import *
from commons import *

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
        runner = workrunner.wlrunner.WorkloadRunner(conf)
        event_iter = runner.run()
    elif workload_src == LBAGENERATOR:
        lbagen = eval("""workrunner.lbaworkloadgenerator.{classname}(conf)""".\
            format(classname=conf['lba_workload_class']))
        event_iter = lbagen
    elif workload_src == LBAMULTIPROC:
        lbagen = eval("""workrunner.lbaworkloadgenerator.{classname}(conf)""".\
            format(classname=conf['lba_workload_class']))
        event_iter = lbagen.get_iter_list()
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
    return eval("ssdbox.simulator.{sim_class}(conf, event_iter)".format(
        sim_class = simulator_class))

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

        conf['simulator_class'] = 'SimulatorNonDESe2e'
        conf['ftl_type'] = "dftlext"

        conf['flash_config']['page_size'] = 1024
        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / conf.page_size)
        conf['dftl']['mapping_cache_bytes'] = int(entries_need * conf['cache_entry_bytes']) # 8 bytes (64bits) needed in mem
        conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

        # perf
        conf['wrap_by_perf'] = False
        conf['perf']['perf_path'] = '/mnt/sdb1/linux-4.1.5/tools/perf/perf'
        conf['perf']['flamegraph_dir'] = '/users/jhe/flamegraph'

        conf['enable_blktrace'] = True
        conf['enable_simulation'] = True

        MOpt = workrunner.filesystem.MountOption
        conf["mnt_opts"]["ext4"]['discard'] = MOpt(opt_name = "discard",
                                         value = "nodiscard",
                                         include_name = False)

        if conf['use_fs']:
            conf['workload_class'] = 'FIO'
            conf['filesystem'] = para['fs']

            if para['fs'] == 'ext4':
                disable_ext4_journal(conf)
                # enable_ext4_journal(conf)

            conf['dev_size_mb'] = para['dev_mb']
            runtime_update(conf)
            workflow(conf)
        else:
            runtime_update(conf)
            fio = workrunner.workload.FIO(conf, 'workload_conf')
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

    wrapper = workrunner.traceandreplay.BlktraceWrapper(
            "/dev/sdc1", "", record_dir, "BlktraceRunnerAlone")
    wrapper.wrapped_run(test_func_run)

    btt(record_dir, 'sdc1')


class DftlextExp001(Experiment):
    def __init__(self):
        # Get default setting
        self.conf = ssdbox.dftlext.Config()
        self.devsize_mb = 128

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = True)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

        self.conf['filesystem'] = 'ext4'
        self.conf['dev_size_mb'] = self.devsize_mb

        self.conf['device_path'] = "/dev/loop0"

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
                bssplit = workrunner.fio.HIDE_ATTR
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
        self.conf.mapping_cache_bytes = int(entries_need * self.conf['cache_entry_bytes']) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(self.devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

def DftlextExp001_run():
    obj = DftlextExp001()
    obj.main()

def chain_items_as_str(iterator):
    return '.'.join([str(x) for x in iterator])

def str_as_filename(s):
    """
    valid_chars
    '-_.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    """
    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in s if c in valid_chars)

def chain_items_as_filename(iterator):
    s = chain_items_as_str(iterator)
    return str_as_filename(s)

def get_expname():
    ret = raw_input("Enter expname (default-expname):")
    if ret == "":
        return "default-expname"
    else:
        return ret

class FIO_DFTLDES(object):
    def __init__(self, parameters):
        # Get default setting
        self.conf = ssdbox.dftldes.Config()
        self.devsize_mb = 256
        self.parameters = parameters
        print self.parameters

    def setup_environment(self):
        set_exp_metadata(self.conf, save_data = True,
                expname = self.parameters.expname,
                subexpname = chain_items_as_str(self.parameters))

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

        self.conf['filesystem'] = self.parameters.filesystem
        self.conf['dev_size_mb'] = self.devsize_mb

        self.conf['device_path'] = "/dev/loop0"

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = 64
        # left to be set later
        self.conf['flash_config']['n_blocks_per_plane'] = None

        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 32

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        filesize = 64*MB
        job_desc = testfio.build_one_run(
                pattern_tuple = [self.parameters.pattern],
                bs = 32*KB, usefs = True, conf = self.conf,
                traffic_size = 64*MB,
                file_size = filesize,
                fdatasync = 1,
                bssplit = workrunner.fio.HIDE_ATTR
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

        self.conf['SSDFramework']['ncq_depth'] = 16

        entries_need = int(self.devsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf.mapping_cache_bytes = int(entries_need * self.conf['cache_entry_bytes']) # 8 bytes (64bits) needed in mem
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
    Parameters = collections.namedtuple("Parameters",
            "filesystem expname pattern")
    expname = get_expname()
    for filesystem in ('ext4'):
        for pattern in ('write'):
            obj = FIO_DFTLDES(Parameters(filesystem = filesystem,
                expname = expname, pattern = pattern))
            obj.main()


def run_ncqexp():
    class NCQExp(object):
        def __init__(self, ncq_depth, n_channels, trafficsize, expname, mode,
                cache_ratio, devsize_mb):
            self.ncq_depth = ncq_depth
            self.n_channels = n_channels
            self.trafficsize = trafficsize
            self.expname = expname
            self.mode = mode
            self.cache_ratio = cache_ratio
            self.devsize_mb = devsize_mb

        def setup_config(self):
            self.conf = ssdbox.dftldes.Config()
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

            entries_need = int(self.devsize_mb * 2**20 * self.cache_ratio / \
                    self.conf['flash_config']['page_size'])
            self.conf.mapping_cache_bytes = int(entries_need * self.conf['cache_entry_bytes']) # 8 bytes (64bits) needed in mem
            self.conf.set_flash_num_blocks_by_bytes(int(self.devsize_mb * 2**20 * 2))
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

    expname = get_expname()
    para_dict = {
            'ncq_depth'     : [1, 32],
            'n_channels'    : [2, 64],
            'mode'          : ['random'],
            'cache_ratio'   : [0.03, 10],
            'trafficsize'   : [64*MB],
            'expname'       : [expname],
            'devsize_mb'    : [256]
            }

    parameter_combs = ParameterCombinations(para_dict)
    for para in parameter_combs:
        exp = NCQExp(**para)
        exp.test_main()


def main(cmd_args):
    if cmd_args.git == True:
        shcmd("sudo -u jun git commit -am 'commit by Makefile: {commitmsg}'"\
            .format(commitmsg=cmd_args.commitmsg \
            if cmd_args.commitmsg != None else ''), ignore_error=True)
        shcmd("sudo -u jun git pull")
        shcmd("sudo -u jun git push")

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





