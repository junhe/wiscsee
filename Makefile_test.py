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
        self.conf['simulator_class'] = 'SimulatorNonDESe2elba'

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
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
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

class DftlextExp2(Experiment):
    """
    This one is for testing the new extent interface
    """
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

class DftlextExpE2e(Experiment):
    """
    This one is for testing the new extent interface with e2e data test
    """
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

class DftlextExpFTLONLY(Experiment):
    """
    This one is for testing the new extent interface with e2e data test
    """
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

        runtime_update(self.conf)

        self.rec = FtlSim.recorder.Recorder(
            output_target = self.conf['output_target'],
            path = self.conf.get_output_file_path(),
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )

        self.ftl = FtlSim.dftlext.Dftl(self.conf, self.rec,
            FtlSim.flash.Flash(recorder = self.rec, confobj = self.conf))

    def run(self):
        # print 'rrrrrrrrread', self.ftl.sec_read(0, 3)
        n = 512 * 8
        print 'wrrrrrrrrrite', self.ftl.sec_write(0, n, data = list(range(n)))
        # print 'flash data', self.ftl.flash.data
        self.ftl.sec_read(0, n)

class DftlTest(unittest.TestCase):
    def test_Dftl(self):
        exp = DftlExp()
        exp.main()


class DftlextTest(unittest.TestCase):
    def test_Dftl(self):
        exp = DftlextExp()
        exp.main()

class DftlextTest2(unittest.TestCase):
    def test_extent(self):
        exp = DftlextExp2()
        exp.main()

class DftlextTest3(unittest.TestCase):
    def test_extent_e2e(self):
        exp = DftlextExpE2e()
        exp.main()


class DftlextTestFTLONLY(unittest.TestCase):
    def test_ftl_only(self):
        exp = DftlextExpFTLONLY()
        exp.main()


# class DftlDESTest(unittest.TestCase):
    # def test_DftlDES(self):
        # exp = DftlDESExp()
        # exp.main()


# class DmftlDESTest(unittest.TestCase):
    # def test_Dmftl(self):
        # exp = DmftlDESExp()
        # exp.main()


def main():
    unittest.main()

if __name__ == '__main__':
    main()

