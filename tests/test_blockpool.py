import unittest

# from Makefile import *
from utilities.utils import *
from ssdbox.blkpool import *
import ssdbox

def create_config():
    conf = ssdbox.dftldes.Config()
    conf['SSDFramework']['ncq_depth'] = 1

    conf['flash_config']['n_pages_per_block'] = 64
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    logicsize_mb = 64
    conf.n_cache_entries = conf.n_mapping_entries_per_page
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    runtime_update(conf)

    return conf


def create_blockpool(conf):
    return ssdbox.dftldes.BlockPool(conf)


class TestBlockPool_data(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = ssdbox.dftlext.Config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        # pop two block from each channel
        k = 2
        blocks = []
        for i in range(n_channels * k):
            blk = block_pool.pop_a_free_block_to_trans()
            blocks.append(blk)

        for block in blocks:
            block_pool.move_used_trans_block_to_free(block)

        for i in range(n_channels):
            self.assertEqual(
                block_pool.count_blocks(tag=TFREE, channels=[i]),
                n_blocks_per_channel)
            self.assertEqual(
                block_pool.count_blocks(tag=TTRANS, channels=[i]), 0)

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
        self.conf = ssdbox.dftlext.Config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        k = 2
        for i in range(n_channels * k):
            block_pool.next_data_page_to_program()

        # each channel now has 2 less blocks
        nblocks_used = (k + self.conf['flash_config']['n_pages_per_block'] - 1) / \
            self.conf['flash_config']['n_pages_per_block']
        for i in range(n_channels):
            self.assertEqual(block_pool.count_blocks(tag=TFREE, channels=[i]),
                n_blocks_per_channel - nblocks_used)
            self.assertEqual(
                block_pool.count_blocks(tag=TDATA, channels=[i]), nblocks_used)

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
        self.conf = ssdbox.dftlext.Config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        k = 2
        for i in range(n_channels * k):
            block_pool.next_gc_data_page_to_program()

        # each channel now has 2 less blocks
        nblocks_used = (k + self.conf['flash_config']['n_pages_per_block'] - 1) / \
            self.conf['flash_config']['n_pages_per_block']
        for i in range(n_channels):
            self.assertEqual(
                block_pool.count_blocks(tag=TFREE, channels=[i]),
                n_blocks_per_channel - nblocks_used)
            self.assertEqual(
                block_pool.count_blocks(tag=TDATA, channels=[i]),
                nblocks_used)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


def create_config():
    conf = ssdbox.dftldes.Config()

    conf['flash_config']['n_pages_per_block'] = 64
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')
    runtime_update(conf)

    return conf


class TestBlockPool_stripping(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = create_config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def ppn_to_channel(self, ppn):
        return ppn / self.conf.n_pages_per_channel

    def my_run1(self):
        runtime_update(self.conf)

        self.conf['stripe_size'] = 2
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        block_pool.pool._next_channel = 0
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel
        block_pool.pool._next_channel = 0

        n = 3
        ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                n = n)
        self.assertEqual(len(ppns_to_write), n)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[0]), 0)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[1]), 0)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[2]), 1)

        ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                n = n)
        self.assertEqual(len(ppns_to_write), n)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[0]), 2)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[1]), 2)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[2]), 3)

    def my_run2(self):
        runtime_update(self.conf)

        self.conf['stripe_size'] = 1
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        block_pool.pool._next_channel = 0
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        n = 5
        ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                n = n)
        self.assertEqual(len(ppns_to_write), n)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[0]), 0)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[1]), 1)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[2]), 2)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[3]), 3)
        self.assertEqual(self.ppn_to_channel(ppns_to_write[4]), 0)

    def my_run3_inf_stripesize(self):
        runtime_update(self.conf)

        self.conf['stripe_size'] = float('inf')
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        block_pool.pool._next_channel = 0
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        n = 5
        ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                n = n)
        self.assertEqual(len(ppns_to_write), n)
        for i in range(n):
            self.assertEqual(self.ppn_to_channel(ppns_to_write[i]), 0)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run1()
        self.my_run2()
        self.my_run3_inf_stripesize()


class TestBlockPool_outofspace(unittest.TestCase):
    """
    Test pop_a_free_block_data
    """
    def setup_config(self):
        self.conf = create_config()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def ppn_to_channel(self, ppn):
        return ppn / self.conf.n_pages_per_channel

    def my_run_1(self):
        runtime_update(self.conf)
        self.conf['stripe_size'] = 2
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        n = self.conf.total_num_pages()

        ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                n = n)

        # should not have exception

    def my_run_2(self):
        runtime_update(self.conf)
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        n = self.conf.total_num_pages() + 1

        with self.assertRaises(ssdbox.blkpool.OutOfSpaceError):
            ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                    n = n)

    def my_run_3(self):
        runtime_update(self.conf)
        self.conf['stripe_size'] = float('inf')
        block_pool = ssdbox.blkpool.BlockPool(self.conf)
        n_channels = block_pool.n_channels
        n_blocks_per_channel = self.conf.n_blocks_per_channel

        n = self.conf.total_num_pages() + 1

        with self.assertRaises(ssdbox.blkpool.OutOfSpaceError):
            ppns_to_write = block_pool.next_n_data_pages_to_program_striped(
                n = n)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run_1()
        self.my_run_2()
        self.my_run_3()




def main():
    unittest.main()

if __name__ == '__main__':
    main()

