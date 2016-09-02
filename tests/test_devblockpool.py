import unittest
from ssdbox.devblockpool import *


TDATA = 'TDATA'
TTRANS = 'TTRANS'


class TestMultiChannelBlockPool(unittest.TestCase):
    def test_init(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

    def test_count_blocks(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        self.assertEqual(pool.count_blocks(tag=TFREE), 8*64)

    def test_count_channel_blocks(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        self.assertEqual(pool.count_blocks(tag=TFREE, channels=[0]), 64)

    def test_change_tag(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        pool.change_tag(0, src=TFREE, dst=TDATA)
        blocks = pool.get_blocks_of_tag(tag=TDATA)
        self.assertListEqual(blocks, [0])

    def test_change_tag_2(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        blocks = []
        for i in range(15):
            block = pool.pick_and_move(src=TFREE, dst=TDATA)
            blocks.append(block)

        self.assertEqual(len(set(blocks)), 15)

        for block in blocks:
            pool.change_tag(block, src=TDATA, dst=TFREE)

    def test_pick_and_move(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        blocknum = pool.pick_and_move(src=TFREE, dst=TDATA)
        self.assertEqual(pool.count_blocks(tag=TFREE), 8*64-1)
        self.assertEqual(pool.count_blocks(tag=TDATA), 1)

        datablocks = pool.get_blocks_of_tag(TDATA)
        self.assertListEqual(datablocks, [blocknum])

    def test_pick(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        blocknum = pool.pick(tag=TFREE)
        self.assertIn(blocknum, pool.get_blocks_of_tag(tag=TFREE))

    def test_pick_failure(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        blocknum = pool.pick(tag=TDATA)
        self.assertEqual(blocknum, None)

    def test_pick_from_channel(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        blocknum = pool.pick(tag=TFREE, channel_id=2)
        self.assertIn(blocknum, pool.get_blocks_of_tag(tag=TFREE,
            channel_id=2))

    def test_pick_from_channel_failure(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        blocknum = pool.pick(tag=TDATA, channel_id=2)
        self.assertEqual(blocknum, None)

    def test_cur_blocks(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

    def test_next_ppns_in_channel(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        ppns = pool._next_ppns_in_channel(
                channel_id=1, n=1, tag=TDATA, block_index=0)
        self.assertEqual(len(ppns), 1)

        other_ppns = pool._next_ppns_in_channel(
                channel_id=1, n=100000, tag=TDATA, block_index=0)
        self.assertEqual(len(other_ppns), 64*32-1)

        more_ppns = pool._next_ppns_in_channel(
                channel_id=1, n=1, tag=TDATA, block_index=0)
        self.assertEqual(len(more_ppns), 0)

    def test_next_ppns_in_channel_2_tags(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        ppns = pool._next_ppns_in_channel(
                channel_id=0, n=1, tag=TDATA, block_index=0)
        self.assertEqual(len(ppns), 1)

        other_ppns = pool._next_ppns_in_channel(
                channel_id=0, n=10, tag=TTRANS, block_index=0)
        self.assertEqual(len(other_ppns), 10)

        # not in the same block
        self.assertNotEqual(ppns[0]/32, other_ppns[0]/32)

    def test_next_ppns(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        # all from one channel
        ppns = pool.next_ppns(n=2, tag=TDATA, block_index=0, stripe_size=2)
        self.assertEqual(len(ppns), 2)

        cur_blocks = pool.current_blocks()
        self.assertEqual(len(cur_blocks), 1)
        cur_block_num = cur_blocks[0]

        for ppn in ppns:
            self.assertEqual(ppn / 32, cur_block_num)

    def test_next_ppns_across_channel(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        # should use 3 channels
        ppns = pool.next_ppns(n=6, tag=TDATA, block_index=0, stripe_size=2)
        self.assertEqual(len(ppns), 6)

        cur_blocks = pool.current_blocks()
        channels = set()
        for block in cur_blocks:
            channel_id, blockoff = pool._global_to_channel(block)
            channels.add(channel_id)
        self.assertEqual(len(channels), 3)

        # ppns are from the current blocks
        for ppn in ppns:
            self.assertIn(ppn / 32, cur_blocks)

    def test_remove_full_cur_blocks(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        # should use 1 block in each channel
        ppns = pool.next_ppns(n=8*32, tag=TDATA, block_index=0, stripe_size=1)
        self.assertEqual(len(ppns), 8*32)

        cur_blocks = pool.current_blocks()
        channels = set()
        for block in cur_blocks:
            channel_id, blockoff = pool._global_to_channel(block)
            channels.add(channel_id)
        self.assertEqual(len(channels), 8)

        # ppns are from the current blocks
        for ppn in ppns:
            self.assertIn(ppn / 32, cur_blocks)

        pool.remove_full_cur_blocks()
        self.assertEqual(len(pool.current_blocks()), 0)

    def test_next_ppns_wrap_around(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        ppns = pool.next_ppns(n=10, tag=TDATA, block_index=0, stripe_size=1)
        self.assertEqual(len(ppns), 10)

        cur_blocks = pool.current_blocks()
        self.assertEqual(len(cur_blocks), 8)
        channels = set()
        for block in cur_blocks:
            channel_id, blockoff = pool._global_to_channel(block)
            channels.add(channel_id)
        self.assertEqual(len(channels), 8)

        for ppn in ppns:
            self.assertIn(ppn / 32, cur_blocks)

    def test_next_ppns_wrap_around(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        ppns = pool.next_ppns(n=10, tag=TDATA, block_index=0, stripe_size=1)
        self.assertEqual(len(set(ppns)), 10)

    def test_block_conversion(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        self.assertEqual(pool._channel_to_global(0, 0), 0)
        channel_id, block_off = pool._global_to_channel(0)
        self.assertEqual(channel_id, 0)
        self.assertEqual(block_off, 0)

        self.assertEqual(pool._channel_to_global(1, 0), 64)
        channel_id, block_off = pool._global_to_channel(64)
        self.assertEqual(channel_id, 1)
        self.assertEqual(block_off, 0)

        self.assertEqual(pool._channel_to_global(3, 4), 3*64+4)
        channel_id, block_off = pool._global_to_channel(3*64+4)
        self.assertEqual(channel_id, 3)
        self.assertEqual(block_off, 4)

    def test_ppn_conversion(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])
        ppn = pool._ppn_channel_to_global(1, 2)
        self.assertEqual(ppn, 64*32 + 2)

    def test_ppn_conversion2(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        # should use 3 channels
        ppns = pool.next_ppns(n=6, tag=TDATA, block_index=0, stripe_size=2)
        self.assertEqual(len(ppns), 6)

        # ppns should from 3 channels
        channel_set = set()
        for ppn in ppns:
            channel = ppn / (64*32)
            channel_set.add(channel)

        self.assertEqual(len(channel_set), 3)

    def test_block_conversion2(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])
        pool._next_channel = 0

        block0 = pool.pick_and_move(src=TFREE, dst=TDATA)
        block1 = pool.pick_and_move(src=TFREE, dst=TDATA)

        channel0 = block0 / pool.n_blocks_per_channel
        channel1 = block1 / pool.n_blocks_per_channel

        self.assertEqual(channel0, 0)
        self.assertEqual(channel1, 1)


class TestGettingDistribution(unittest.TestCase):
    def test(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])

        dist = pool.get_erasure_count_dist()

        self.assertEqual(dist[0], 64*8)

    def test_erase_some(self):
        pool = MultiChannelBlockPool(
                n_channels=8,
                n_blocks_per_channel=64,
                n_pages_per_block=32,
                tags=[TDATA, TTRANS])


        used_blocks = []
        for i in range(5):
            block = pool.pick_and_move(TFREE, TDATA)
            used_blocks.append(block)

        for block in used_blocks:
            pool.change_tag(block, TDATA, TFREE)

        dist = pool.get_erasure_count_dist()
        self.assertEqual(dist[0], 64*8-5)
        self.assertEqual(dist[1], 5)



def main():
    unittest.main()

if __name__ == '__main__':
    main()



