import unittest
import collections
import shutil
import os

from config import LBAGENERATOR
from workflow import *
import ssdbox
from utilities import utils
from ssdbox.hostevent import Event, ControlEvent
from benchmarks import rule_parameter
from pyreuse.helpers import shcmd
from benchmarks import experiment


def create_config():
    conf = ssdbox.dftldes.Config()
    conf['SSDFramework']['ncq_depth'] = 1

    conf['flash_config']['n_pages_per_block'] = 64
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    # set ftl
    conf['do_not_check_gc_setting'] = True
    conf.GC_high_threshold_ratio = 0.96
    conf.GC_low_threshold_ratio = 0

    conf['enable_simulation'] = True

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    conf['ftl_type'] = 'dftldes'
    conf['simulator_class'] = 'SimulatorDESNew'

    logicsize_mb = 16
    conf.n_cache_entries = conf.n_mapping_entries_per_page * 16
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


def on_fs_config(conf):
    # environment
    conf['device_path'] = "/dev/loop0"
    conf['dev_size_mb'] = 16
    conf['filesystem'] = "ext4"
    conf["n_online_cpus"] = 'all'

    conf['linux_ncq_depth'] = 31

    # workload
    conf['workload_class'] = 'SimpleRandReadWrite'

class TestWorkflow(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        wf = Workflow(conf)

    def test_save_conf(self):
        conf = create_config()
        conf['result_dir'] = '/tmp/'
        jsonpath = os.path.join(conf['result_dir'], 'config.json')

        if os.path.exists(jsonpath):
            os.remove(jsonpath)

        wf = Workflow(conf)
        wf._save_conf()

        self.assertTrue(os.path.exists(jsonpath))


    def test_onfs_workload(self):
        conf = create_config()
        on_fs_config(conf)

        datapath = os.path.join(conf["fs_mount_point"], 'datafile')
        if os.path.exists(datapath):
            os.remove(datapath)

        wf = Workflow(conf)
        wf.run_workload()

        self.assertTrue(os.path.exists(datapath))

    def test_simulation(self):
        conf = create_config()

        ctrl_event = ControlEvent(OP_ENABLE_RECORDER)
        event = Event(512, 0, OP_WRITE, 0, 4096)

        wf = Workflow(conf)
        wf.run_simulator([ctrl_event, event])

    def test_on_fs_run_and_sim(self):
        conf = create_config()
        on_fs_config(conf)
        conf['enable_blktrace'] = True

        datapath = os.path.join(conf["fs_mount_point"], 'datafile')
        if os.path.exists(datapath):
            os.remove(datapath)

        wf = Workflow(conf)
        wf.run()

        self.assertTrue(os.path.exists(datapath))




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
                self.conf['workload_src'] = LBAGENERATOR
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
                self.conf["workload_src"] = LBAGENERATOR
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
                self.conf["workload_src"] = LBAGENERATOR
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


if __name__ == '__main__':
    unittest.main()

