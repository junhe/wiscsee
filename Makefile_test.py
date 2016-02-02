import unittest
import pprint

from Makefile import *



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
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)


class DftlextExp(Experiment):
    def __init__(self):
        # Get default setting
        self.conf = config.ConfigNewFlash()

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
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)


class DftlextExp2(Experiment):
    """
    This one is for testing the new extent interface
    """
    def __init__(self):
        # Get default setting
        self.conf = config.ConfigNewFlash()

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
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf.page_size)
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)

class DftlextExpE2e(Experiment):
    """
    This one is for testing the new extent interface with e2e data test
    """
    def __init__(self):
        # Get default setting
        self.conf = config.ConfigNewFlash()

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
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
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

class DftlextTest2(unittest.TestCase):
    def test_extent(self):
        exp = DftlextExp2()
        exp.main()

class DftlextTest3(unittest.TestCase):
    def test_extent_e2e(self):
        exp = DftlextExpE2e()
        exp.main()

class TestChannelBlockPool(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        channel_pool = FtlSim.dftlext.ChannelBlockPool(self.conf, 0)
        channel_pool.pop_a_free_block_to_trans()
        self.assertEqual(len(channel_pool.trans_usedblocks), 1)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestBlockPool_freeblocks(unittest.TestCase):
    """
    Test pop_a_free_block
    """
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = FtlSim.dftlext.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        # pop two block from each channel
        k = 2
        for i in range(n_channels * k):
            block_pool.pop_a_free_block()

        # each channel now has 2 less blocks
        for i in range(n_channels):
            self.assertEqual(len(block_pool.channel_pools[i].freeblocks),
                n_blocks_per_channel - k)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestBlockPool_data(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = FtlSim.dftlext.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        # pop two block from each channel
        k = 2
        blocks = []
        for i in range(n_channels * k):
            blk = block_pool.pop_a_free_block_to_trans()
            blocks.append(blk)

        # each channel now has 2 less blocks
        for i in range(n_channels):
            self.assertEqual(len(block_pool.channel_pools[i].freeblocks),
                n_blocks_per_channel - k)
            self.assertEqual(
                len(block_pool.channel_pools[i].trans_usedblocks), k)

        for block in blocks:
            block_pool.move_used_trans_block_to_free(block)

        # each channel now has 2 less blocks
        for i in range(n_channels):
            self.assertEqual(len(block_pool.channel_pools[i].freeblocks),
                n_blocks_per_channel)
            self.assertEqual(
                len(block_pool.channel_pools[i].trans_usedblocks), 0)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestBlockPool_trans(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = FtlSim.dftlext.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        # pop two block from each channel
        k = 2
        for i in range(n_channels * k):
            block_pool.pop_a_free_block_to_data()

        # each channel now has 2 less blocks
        for i in range(n_channels):
            self.assertEqual(len(block_pool.channel_pools[i].freeblocks),
                n_blocks_per_channel - k)
            self.assertEqual(
                len(block_pool.channel_pools[i].data_usedblocks), k)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestBlockPool_next_data(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = FtlSim.dftlext.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        k = 2
        for i in range(n_channels * k):
            block_pool.next_data_page_to_program()

        # each channel now has 2 less blocks
        nblocks_used = (k + self.conf['flash_config']['n_pages_per_block'] - 1) / \
            self.conf['flash_config']['n_pages_per_block']
        for i in range(n_channels):
            self.assertEqual(len(block_pool.channel_pools[i].freeblocks),
                n_blocks_per_channel - nblocks_used)
            self.assertEqual(
                len(block_pool.channel_pools[i].data_usedblocks), nblocks_used)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestBlockPool_next_gc_data(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = FtlSim.dftlext.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        k = 2
        for i in range(n_channels * k):
            block_pool.next_gc_data_page_to_program()

        # each channel now has 2 less blocks
        nblocks_used = (k + self.conf['flash_config']['n_pages_per_block'] - 1) / \
            self.conf['flash_config']['n_pages_per_block']
        for i in range(n_channels):
            self.assertEqual(len(block_pool.channel_pools[i].freeblocks),
                n_blocks_per_channel - nblocks_used)
            self.assertEqual(
                len(block_pool.channel_pools[i].data_usedblocks), nblocks_used)

        print block_pool.used_ratio()

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestDftextGC(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()
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
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

        devsize_mb = 16
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


class TestDftextGCSingleChannel(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

        print '1', self.conf.n_blocks_per_dev

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
        self.conf['ftl_type'] = 'dftlext'
        self.conf['simulator_class'] = 'SimulatorNonDESe2e'

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


class TestDftlextTimeline(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        tl = FtlSim.dftlext.Timeline(self.conf)
        tl.turn_on()
        tl.add_logical_op(0, 100, FtlSim.dftlext.LOGICAL_READ)
        tl.incr_time_stamp('flash.read', 3)
        self.assertEqual(tl.table[-1]['end_timestamp'],
            tl.conf['flash_config']['page_read_time'] * 3)
        tl.add_logical_op(200, 100, FtlSim.dftlext.LOGICAL_READ)
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
        self.conf = config.ConfigNewFlash()
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
        rec = FtlSim.recorder.Recorder(output_target = self.conf['output_target'],
            path = self.conf.get_output_file_path(),
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )
        rec.disable()
        fc = FtlSim.dftlext.ParallelFlash(self.conf, rec,
                FtlSim.dftlext.GlobalHelper(self.conf))

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
        self.conf = config.ConfigNewFlash()
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

        devsize_mb = 16
        entries_need = int(devsize_mb * 2**20 * 0.03 / \
                self.conf['flash_config']['page_size'])
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

    def my_run(self):
        self.ftl.global_helper.timeline.turn_on()
        n = 1
        sectors = list(range(n))
        data = [FtlSim.simulator.random_data(sec) for sec in sectors]
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


def main():
    unittest.main()

if __name__ == '__main__':
    main()

