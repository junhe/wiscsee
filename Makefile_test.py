import unittest

from Makefile import *


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

class DftlExp(Experiment):
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftl2'
        self.conf['simulation_processor'] = 'e2e'
        self.conf['simulator_type'] = 'NonDES'
        # self.conf['simulator_type'] = 'DES'

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf['interface_level'] =  'page'
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

class DftlextExp(Experiment):
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "TestWorkload"
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulation_processor'] = 'e2e'
        self.conf['simulator_type'] = 'NonDES'
        # self.conf['simulator_type'] = 'DES'

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf['interface_level'] =  'page'
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)


class DftlDESExp(Experiment):
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlDES'
        self.conf['simulation_processor'] = 'e2e'
        # self.conf['simulator_type'] = 'NonDES'
        self.conf['simulator_type'] = 'DES'

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf['interface_level'] =  'page'
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

class DmftlDESExp(Experiment):
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dmftlDES'
        self.conf['simulation_processor'] = 'extent'
        # self.conf['simulator_type'] = 'NonDES'
        self.conf['simulator_type'] = 'DES'

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf['interface_level'] =  'page'
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)


class DftlTest(unittest.TestCase):
    def test_Dftl(self):
        exp = DftlExp()
        exp.main()

class DftlextTest(unittest.TestCase):
    def test_Dftl(self):
        exp = DftlextExp()
        exp.main()

class DftlDESTest(unittest.TestCase):
    def test_DftlDES(self):
        exp = DftlDESExp()
        exp.main()

class DmftlDESTest(unittest.TestCase):
    def test_Dmftl(self):
        exp = DmftlDESExp()
        exp.main()


def main():
    unittest.main()

if __name__ == '__main__':
    main()

