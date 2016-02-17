import config
import unittest
import simpy

from Makefile import *


class TestFTLwithDFTL(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkloadMANUAL"
        self.conf["lba_workload_configs"]["ExtentTestWorkloadMANUAL"] = {
            "op_count": 100}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 2
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestFTLwithDFTL2(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 32
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 8

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload4DFTLDES"
        self.conf["lba_workload_configs"]["ExtentTestWorkload4DFTLDES"] = {
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 8
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestFTLwithDFTLFLEX(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkloadFLEX"
        self.conf["lba_workload_configs"]["ExtentTestWorkloadFLEX"] = {
                "op_count": 100, "ops":['discard', 'write']}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 2
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestFTLwithDFTLFLEX2(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 1

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        w = 'write'
        r = 'read'
        d = 'discard'

        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkloadFLEX2"
        self.conf["lba_workload_configs"]["ExtentTestWorkloadFLEX2"] = {
                "events": [
                    (d, 1, 3),
                    (w, 1, 3),
                    (d, 1, 3)
                    ]}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 2
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestFTLwithDFTLIntegrated(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 32
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 8

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))
        print "Current n_blocks_per_plane",\
            self.conf['flash_config']['n_blocks_per_plane']

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def _test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestFTLwithDFTLIntegrated2(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = 4
        self.conf['flash_config']['n_blocks_per_plane'] = 8
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4


    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "TestWorkloadFLEX3"

        traffic = 10*MB
        chunk_size = 32*KB
        page_size = self.conf['flash_config']['page_size']
        self.conf["lba_workload_configs"]["TestWorkloadFLEX3"] = {
                "op_count": traffic/chunk_size,
                "extent_size": chunk_size/page_size ,
                "ops": ['write'], 'mode': 'random'}
                # "ops": ['read', 'write', 'discard']}
        print self.conf['lba_workload_configs']['TestWorkloadFLEX3']
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 8
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.3))
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

class TestFTLwithDFTLGCthreshold(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = 64
        self.conf['flash_config']['n_blocks_per_plane'] = 8
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4


    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "TestWorkloadFLEX3"

        traffic = 10*MB
        chunk_size = 32*KB
        page_size = self.conf['flash_config']['page_size']
        self.conf["lba_workload_configs"]["TestWorkloadFLEX3"] = {
                "op_count": traffic/chunk_size,
                "extent_size": chunk_size/page_size ,
                "ops": ['write'], 'mode': 'random'}
                # "ops": ['read', 'write', 'discard']}
        print self.conf['lba_workload_configs']['TestWorkloadFLEX3']
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 8
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.3))
        print "Current n_blocks_per_plane",\
            self.conf['flash_config']['n_blocks_per_plane']

    def my_run(self):
        runtime_update(self.conf)
        with self.assertRaisesRegexp(RuntimeError,
                "Num of spare blocks \S+ may not be enough"\
                "for garbage collection. You may encounter "\
                "Out Of Space error!"):
            workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()




def main():
    unittest.main()

if __name__ == '__main__':
    main()





