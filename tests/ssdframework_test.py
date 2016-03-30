import unittest
import simpy

from Makefile import *


class TestFTLwithDFTL(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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
        self.conf = ssdbox.dftldes.Config()
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
            "op_count": 100}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 8
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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
        self.conf = ssdbox.dftldes.Config()
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

        traffic = 2*MB
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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

class Test_load_translation_page(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.dftldes.Config()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = 4
        self.conf['flash_config']['n_blocks_per_plane'] = 8
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4
        self.conf

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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.3))
        print "Current n_blocks_per_plane",\
            self.conf['flash_config']['n_blocks_per_plane']

    def process(self, simulator):
        ftl = simulator.ssdframework.realftl

        new_mappings = {lpn: lpn * 1000 for lpn in range(100)}
        yield simulator.env.process(
            ftl.mapping_manager.update_translation_page_on_flash(0,
                new_mappings, ''))

        entries = ftl.mapping_manager.retrieve_translation_page(0)

        for lpn in range(4):
            self.assertEqual(entries[lpn], lpn*1000)

    def my_run(self):
        runtime_update(self.conf)
        simulator = create_simulator(self.conf['simulator_class'],
                self.conf, [] )

        simulator.env.process(self.process(simulator))

        simulator.env.run()

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class Test_translation_cache(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = int(self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes']) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.3))
        print "Current n_blocks_per_plane",\
            self.conf['flash_config']['n_blocks_per_plane']

    def my_run(self):
        runtime_update(self.conf)
        simulator = create_simulator(self.conf['simulator_class'],
                self.conf, [] )
        ftl = simulator.ssdframework.realftl
        ftl.mapping_manager.mapping_table.add_new_entry_if_not_exist(0, 333,
                dirty = True)

        ppn = ftl.mapping_manager.mapping_table.lpn_to_ppn(0)
        self.assertEqual(ppn, 333)

        # check it if overwrites the existing one
        ftl.mapping_manager.mapping_table.add_new_entry_if_not_exist(0, 444,
                dirty = True)
        self.assertEqual(ppn, 333)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestDatacache(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.dftldes.Config()
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
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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


class TestSimulatorSync(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.dftldes.Config()
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

        self.conf["workload_src"] = LBAMULTIPROC
        self.conf["lba_workload_class"] = "MultipleProcess"
        self.conf["lba_workload_configs"]["MultipleProcess"] = {
                "events": [
                    [(d, 1, 3),
                    (w, 1, 3),
                    (d, 1, 3)],

                    [(d, 2, 3),
                    (w, 3, 3),
                    (d, 4, 3)]
                    ]}

        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDESSync'

        devsize_mb = 2
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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


class TestVPNResourcePool(unittest.TestCase):

    def access_vpn(self, env, respool, vpn):
        req = respool.get_request(vpn)
        yield req
        yield env.timeout(10)
        respool.release_request(vpn, req)

    def init_proc(self, env, respool):
        procs = []
        for i in range(3):
            p = env.process(self.access_vpn(env, respool, 88))
            procs.append(p)

            p = env.process(self.access_vpn(env, respool, 89))
            procs.append(p)

        yield simpy.events.AllOf(env, procs)
        self.assertEqual(env.now, 30)

    def test_request(self):
        env = simpy.Environment()
        respool = ssdbox.dftldes.VPNResourcePool(env)

        env.process(self.init_proc(env, respool))
        env.run()


class TestParallelDFTL(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.dftldes.Config()
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
                    (r, 1, 3)
                    ]}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDESNew'

        devsize_mb = 2
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page \
                * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
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


def main():
    unittest.main()

if __name__ == '__main__':
    main()





