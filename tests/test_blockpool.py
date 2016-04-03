import unittest

from Makefile import *


class TestChannelBlockPool(unittest.TestCase):
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
        channel_pool = ssdbox.dftlext.ChannelBlockPool(self.conf, 0)
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
        block_pool = ssdbox.dftlext.BlockPool(self.conf)
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
        block_pool = ssdbox.dftlext.BlockPool(self.conf)
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
        block_pool = ssdbox.dftlext.BlockPool(self.conf)
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
        block_pool = ssdbox.dftlext.BlockPool(self.conf)
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
        block_pool = ssdbox.dftlext.BlockPool(self.conf)
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


def main():
    unittest.main()

if __name__ == '__main__':
    main()

