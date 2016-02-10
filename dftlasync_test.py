import config
import unittest
import simpy

from Makefile import *

class TestNCQ(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigAsyncFTL()
        self.conf['dftlasync']['ncq_depth'] = 8

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        ncq = FtlSim.dftlasync.NativeCommandQueue(
                self.conf['dftlasync']['ncq_depth'],
                simpy.Environment()
                )

        self.assertEqual(ncq.is_empty(3), True)
        ncq.push(element = 'element1', index = 3)
        ncq.push(element = 'element2', index = 3)
        self.assertEqual(ncq.is_empty(3), False)
        self.assertEqual(ncq.peek_tail(index = 3), 'element1')
        ret = ncq.pop(index = 3)
        self.assertEqual(ret, 'element1')

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestFTLNCQ(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigAsyncFTL()
        self.conf['dftlasync']['ncq_depth'] = 8

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        ncq = FtlSim.dftlasync.FTLNCQ(
                self.conf['dftlasync']['ncq_depth'],
                simpy.Environment()
                )

        self.assertEqual(ncq.is_empty(3), True)
        ncq.push(element = 'element1', index = 3)
        ncq.push(element = 'element2', index = 5)

        ret = ncq.next_request()
        self.assertEqual(ret, 'element1')

        ret = ncq.next_request()
        self.assertEqual(ret, 'element2')

        ret = ncq.next_request()
        self.assertEqual(ret, None)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()



class TestDftlasync(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigAsyncFTL()
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
        self.conf['ftl_type'] = 'dftlasync'
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





