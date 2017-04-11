import unittest
import collections
import shutil
import os

import config
from workflow import *
import wiscsim
from utilities import utils
from wiscsim.hostevent import Event, ControlEvent
from config_helper import rule_parameter
from pyreuse.helpers import shcmd
from config_helper import experiment


class Test_TraceOnly(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TraceOnly2", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['enable_simulation'] = False
        para['enable_blktrace'] = True

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class Test_TraceAndSimulateDFTLDES(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TraceAndSimulateDFTLDES_xjjj", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['ftl'] = "dftldes"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class Test_TraceAndSimulateNKFTL(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TraceAndSimulateNKFTL_xjjj", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['ftl'] = "nkftl2"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class Test_SimulateForSyntheticWorkload(unittest.TestCase):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_src'] = config.LBAGENERATOR
                self.conf['lba_workload_class'] = "AccessesWithDist"
                self.conf['AccessesWithDist'] = {
                        'lba_access_dist': 'uniform',
                        'traffic_size': 8*MB,
                        'chunk_size': 64*KB,
                        'space_size': 8*MB,
                        'skew_factor': None,
                        'zipf_alpha': None,
                        }

        para = experiment.get_shared_nolist_para_dict("test_exp_SimulateForSyntheticWorkload", 16*MB)
        para['ftl'] = "nkftl2"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class TestUsingExistingTraceToSimulate(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_src"] = config.LBAGENERATOR
                self.conf["lba_workload_class"] = "BlktraceEvents"
                self.conf['lba_workload_configs']['mkfs_event_path'] = \
                        self.para.mkfs_path
                self.conf['lba_workload_configs']['ftlsim_event_path'] = \
                        self.para.ftlsim_path

        para = experiment.get_shared_nolist_para_dict("test_exp_TestUsingExistingTraceToSimulate_jj23hx", 1*GB)
        para.update({
            'ftl': "dftldes",
            "mkfs_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim-mkfs.txt",
            "ftlsim_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim.txt",
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()




class TestUsingExistingTraceToStudyRequestScale(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_src"] = config.LBAGENERATOR
                self.conf["lba_workload_class"] = "BlktraceEvents"
                self.conf['lba_workload_configs']['mkfs_event_path'] = \
                        self.para.mkfs_path
                self.conf['lba_workload_configs']['ftlsim_event_path'] = \
                        self.para.ftlsim_path

        para = experiment.get_shared_nolist_para_dict("test_exp_TestUsingExistingTraceToStudyRequestScale_jj23hx", 1*GB)
        para.update({
            'ftl': "ftlcounter",
            "mkfs_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim-mkfs.txt",
            "ftlsim_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim.txt",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
            'do_dump_lpn_sem': False,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


###################################################################
# Experiments setting similar to SSD Contract paper
###################################################################

class TestRunningWorkloadAndOutputRequestScale(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TestRequestScale_jjj3nx", 16*MB)
        para['device_path'] = "/dev/loop0"
        para.update(
            {
                'device_path': "/dev/loop0",
                'ftl' : 'ftlcounter',
                'enable_simulation': True,
                'dump_ext4_after_workload': True,
                'only_get_traffic': False,
                'trace_issue_and_complete': True,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class TestLocality(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in rule_parameter.ParaDict("testexpname", ['sqlitewal-update'], "locality"):
            experiment.execute_simulation(para)

class TestAlignment(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in rule_parameter.ParaDict("testexpname", ['sqlitewal-update'], "alignment"):
            experiment.execute_simulation(para)


class TestGrouping(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in rule_parameter.ParaDict("testexpname", ['sqlitewal-update'], "grouping"):
            experiment.execute_simulation(para)


class TestUniformDataLifetime(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TestUniformDataLifetime", 16*MB)
        para.update(
            {
                'ftl' : 'ftlcounter',
                'device_path'    : '/dev/loop0',
                'enable_simulation': True,
                'dump_ext4_after_workload': True,
                'only_get_traffic': False,
                'trace_issue_and_complete': False,
                'gen_ncq_depth_table': False,
                'do_dump_lpn_sem': False,
                'rm_blkparse_events': True,
                'sort_block_trace': False,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


# class Test_TraceAndSimulateLinuxDD(unittest.TestCase):
    # def test_run(self):
        # class LocalExperiment(experiment.Experiment):
            # def setup_workload(self):
                # self.conf['workload_class'] = "LinuxDD"

        # para = experiment.get_shared_nolist_para_dict("test_exp_LinuxDD", 16*MB)
        # para['device_path'] = "/dev/loop0"
        # para['filesystem'] = "ext4"
        # para['ftl'] = "dftldes"
        # Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        # obj = LocalExperiment( Parameters(**para) )
        # obj.main()


if __name__ == '__main__':
    unittest.main()

