import config
import unittest
import simpy

from Makefile import *

class TestDftlncq(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf.n_channels_per_dev = 4

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 100}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlncq'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 1
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


class TestDftlncq2(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()

        # 4 pages per channel, 2 channels in total
        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 2

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
        self.conf['ftl_type'] = 'dftlncq'
        self.conf['simulator_class'] = 'SimulatorDES'

    def my_run(self):
        runtime_update(self.conf)
        workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestDftlncqPhyMachTranslation(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        # 2 pages per block, 2 blocks per channel, 2 channels in total
        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 2

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        rec = FtlSim.recorder.Recorder(
            output_target = self.conf['output_target'],
            path = self.conf.get_output_file_path(),
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )
        rec.disable()
        ftl = FtlSim.dftlncq.FTL(self.conf, rec, None)

        flash_reqs = ftl.get_flash_requests_for_page(0, 5, 'read')
        self.assertEqual(len(flash_reqs), 5)
        for req in flash_reqs:
            self.assertEqual(req.operation, 'OP_READ')
        self.assertEqual(flash_reqs[0].addr.channel, 0)
        self.assertEqual(flash_reqs[4].addr.channel, 1)


        flash_reqs = ftl.get_flash_requests_for_block(0, 9, 'erase')
        self.assertEqual(len(flash_reqs), 9)
        for req in flash_reqs:
            self.assertEqual(req.operation, 'OP_ERASE')
        self.assertEqual(flash_reqs[0].addr.channel, 0)
        self.assertEqual(flash_reqs[2].addr.channel, 1)
        self.assertEqual(flash_reqs[8].addr.channel, 4)

        io_req = FtlSim.simulator.Event(self.conf['sector_size'],
                0, 'read', 0, self.conf.page_size * 5)
        flash_reqs = ftl.get_direct_mapped_flash_requests(io_req)
        self.assertEqual(len(flash_reqs), 5)
        for req in flash_reqs:
            self.assertEqual(req.operation, 'OP_READ')
        self.assertEqual(flash_reqs[0].addr.channel, 0)
        self.assertEqual(flash_reqs[4].addr.channel, 1)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestFTLwithDFTL(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()
        self.conf.n_channels_per_dev = 4
        self.conf['dftlncq']['ncq_depth'] = 2

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
        self.conf['ftl_type'] = 'ftlwdftl'
        self.conf['simulator_class'] = 'SimulatorDES'

        devsize_mb = 1
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


def main():
    unittest.main()

if __name__ == '__main__':
    main()





