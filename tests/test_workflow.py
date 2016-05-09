import unittest
from workflow import *
import ssdbox
from utilities import utils
from ssdbox.hostevent import Event, ControlEvent

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

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    conf['ftl_type'] = 'dftldes'
    conf['simulator_class'] = 'SimulatorDESNew'

    logicsize_mb = 64
    conf.n_cache_entries = conf.n_mapping_entries_per_page
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf

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

        # environment
        conf['device_path'] = "/dev/loop0"
        conf['dev_size_mb'] = 64
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

        datapath = os.path.join(conf["fs_mount_point"], 'datafile')
        if os.path.exists(datapath):
            os.remove(datapath)

        wf = Workflow(conf)
        wf.run_workload()

        self.assertTrue(os.path.exists(datapath))


    def test_simulation(self):
        conf = create_config()

        # set ftl
        conf['do_not_check_gc_setting'] = True
        conf.GC_high_threshold_ratio = 0.96
        conf.GC_low_threshold_ratio = 0

        conf['enable_simulation'] = True

        ctrl_event = ControlEvent(OP_ENABLE_RECORDER)
        event = Event(512, 0, OP_WRITE, 0, 4096)

        wf = Workflow(conf)
        wf.run_simulator([ctrl_event, event])



if __name__ == '__main__':
    unittest.main()

