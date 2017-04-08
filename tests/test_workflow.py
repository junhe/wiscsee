import unittest
import collections
import shutil
from workflow import *
import ssdbox
from utilities import utils
from ssdbox.hostevent import Event, ControlEvent
from benchmarks import expconfs, appbench, filesim
from pyreuse.helpers import shcmd
from benchmarks import experimenter

import os

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
    conf['workload_class'] = 'PatternSuite'
    conf['workload_conf_key'] = 'PatternSuite'
    conf['PatternSuite'] = {'patternname': 'SRandomWrite',
        'parameters': {
            'zone_size': 1*MB,
            'chunk_size': 512*KB,
            'traffic_size': 1*MB,
            }
        }

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



class TestRequestScale(unittest.TestCase):
    def test(self):
        para_pool = expconfs.ParameterPool(
                expname = "tmptest",
                testname = ["sqliteWAL_reqscale_w_rand"],
                filesystem = ['ext4']
                )

        for para in para_pool:
            appbench.run_on_real_dev(para)

class TestUniformDataLifetime(unittest.TestCase):
    def test(self):
        para_pool = expconfs.ParameterPool(
                expname = "tmptest",
                testname = ["sqliteWAL_wearlevel_w_rand"],
                filesystem = ['ext4']
                )

        for para in para_pool:
            appbench.run_on_real_dev(para)

class TestLocality(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in filesim.ParaDict("testexpname", ['sqlitewal-update'], "locality"):
            appbench.execute_simulation(para)

class TestAlignment(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in filesim.ParaDict("testexpname", ['sqlitewal-update'], "alignment"):
            appbench.execute_simulation(para)


class TestGrouping(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in filesim.ParaDict("testexpname", ['sqlitewal-update'], "grouping"):
            appbench.execute_simulation(para)





class Test_TraceOnly2(unittest.TestCase):
    def test_run(self):
        class LocalExperimenter(experimenter.Experimenter):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experimenter.get_shared_nolist_para_dict("test_exp_TraceOnly2", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['enable_simulation'] = False
        para['enable_blktrace'] = True

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperimenter( Parameters(**para) )
        obj.main()


class Test_TraceAndSimulateDFTLDES(unittest.TestCase):
    def test_run(self):
        class LocalExperimenter(experimenter.Experimenter):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experimenter.get_shared_nolist_para_dict("test_exp_TraceAndSimulateDFTLDES_xjjj", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['ftl'] = "dftldes"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperimenter( Parameters(**para) )
        obj.main()


class Test_TraceAndSimulateNKFTL(unittest.TestCase):
    def test_run(self):
        class LocalExperimenter(experimenter.Experimenter):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experimenter.get_shared_nolist_para_dict("test_exp_TraceAndSimulateNKFTL_xjjj", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['ftl'] = "nkftl2"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperimenter( Parameters(**para) )
        obj.main()







if __name__ == '__main__':
    unittest.main()

