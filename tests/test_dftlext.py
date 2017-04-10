import unittest
import pprint

import wiscsim
from workflow import run_workflow
import config
from utilities.utils import *
from commons import *


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


class TestTemplate(unittest.TestCase):
    def setup_config(self):
        self.conf = config.Config()

    def setup_environment(self):
        raise NotImplementedError

    def setup_workload(self):
        raise NotImplementedError

    def setup_ftl(self):
        raise NotImplementedError

    def my_run(self):
        raise NotImplementedError

    def _test_main(self):
        "Remove prefix _"
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class DftlextExp(Experiment):
    def __init__(self):
        # Get default setting
        self.conf = wiscsim.dftlext.Config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = config.LBAGENERATOR
        self.conf["lba_workload_class"] = "TestWorkload"
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        logicsize_mb = 16
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        run_workflow(self.conf)


class DftlextExp2(Experiment):
    """
    This one is for testing the new extent interface
    """
    def __init__(self):
        # Get default setting
        self.conf = wiscsim.dftlext.Config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = config.LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 100}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        logicsize_mb = 16
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        run_workflow(self.conf)

class DftlextExpE2e(Experiment):
    """
    This one is for testing the new extent interface with e2e data test
    """
    def __init__(self):
        # Get default setting
        self.conf = wiscsim.dftlext.Config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = config.LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        logicsize_mb = 16
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        run_workflow(self.conf)

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

class TestDftextGC(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftlext.Config()
        self.conf.n_channels_per_dev = 4

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = config.LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        logicsize_mb = 16
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        run_workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestDftextGCSingleChannel(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftlext.Config()

        print '1', self.conf.n_blocks_per_dev

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = config.LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        logicsize_mb = 1
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        run_workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestDftlextTimeline(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftlext.Config()

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        tl = wiscsim.dftlext.Timeline(self.conf)
        tl.turn_on()
        tl.add_logical_op(0, 100, wiscsim.dftlext.LOGICAL_READ)
        tl.incr_time_stamp('flash.read', 3)
        self.assertEqual(tl.table[-1]['end_timestamp'],
            tl.conf['flash_config']['page_read_time'] * 3)
        tl.add_logical_op(200, 100, wiscsim.dftlext.LOGICAL_READ)
        self.assertEqual(tl.table[-1]['start_timestamp'],
            tl.conf['flash_config']['page_read_time'] * 3)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestDftlextParallelFlash(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftlext.Config()
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
        rec = wiscsim.recorder.Recorder(output_target = self.conf['output_target'],
            output_directory = self.conf['result_dir'],
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )
        rec.disable()
        fc = wiscsim.dftlext.ParallelFlash(self.conf, rec,
                wiscsim.dftlext.GlobalHelper(self.conf))

        self.assertEqual(fc.get_max_channel_page_count(ppns = [0]), 1)
        self.assertEqual(fc.get_max_channel_page_count(ppns = [0, 1, 2, 3]), 4)
        self.assertEqual(fc.get_max_channel_page_count(ppns = [0, 1, 2, 3, 4]), 4)

        ppns = [0, 1, 99]
        data = [100 * ppn for ppn in ppns]

        fc.write_pages(ppns, data, tag = None)
        data_read = fc.read_pages(ppns, tag = None)
        self.assertListEqual(data, data_read)

        fc.erase_blocks([0], tag = None)
        data_read = fc.read_pages(ppns, tag = None)
        self.assertListEqual(data_read, [None, None, 9900])

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestTimelineAndFlash(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftlext.Config()
        # 2 pages per block, 2 blocks per channel, 2 channels in total
        self.conf['sector_size'] = self.conf['flash_config']['page_size']
        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 2

        self.conf['flash_config']['page_read_time'] = 1
        self.conf['flash_config']['page_prog_time'] = 1
        self.conf['flash_config']['block_erase_time'] = 1

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        logicsize_mb = 16
        entries_need = int(logicsize_mb * 2**20 * 0.03 / \
                self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

        runtime_update(self.conf)

        self.rec = wiscsim.recorder.Recorder(
            output_target = self.conf['output_target'],
            output_directory = self.conf['result_dir'],
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )

        self.ftl = wiscsim.dftlext.Dftl(self.conf, self.rec,
            wiscsim.flash.Flash(recorder = self.rec, confobj = self.conf))

    def my_run(self):
        self.ftl.global_helper.timeline.turn_on()
        n = 1
        sectors = list(range(n))
        data = [wiscsim.simulator.random_data(sec) for sec in sectors]
        self.ftl.sec_write(0, n, data = data)

        # a write involve a tranlation page read and data page write
        self.assertEqual(self.ftl.global_helper.timeline.table[-1]\
                ['end_timestamp'], 2)
        self.ftl.sec_read(0, n)
        # a read involve a tranlation page read and data page write
        self.assertEqual(self.ftl.global_helper.timeline.table[-1]\
                ['end_timestamp'], 3)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestEventIter(unittest.TestCase):
    def test_main2(self):
        conf = config.ConfigNewFlash()
        conf["event_file_column_names"] = ['pid', 'operation', 'offset', 'size',
                    'timestamp', 'pre_wait_time', 'sync']
        events = list(wiscsim.hostevent.EventIterator(conf,
            ["1123 write 0 4096 0 0 S", "13 write 40960 4096 1 1 S"]))
        e = events[0]
        self.assertEqual(e.pid, 1123)
        self.assertEqual(e.operation, OP_WRITE)
        self.assertEqual(e.offset, 0)
        self.assertEqual(e.size, 4096)


def main():
    unittest.main()

if __name__ == '__main__':
    main()

