import unittest
from wiscsim.tagblockpool import *


TDATA = 'TDATA'
TTRANS = 'TTRANS'
NPAGESPERBLOCK = 64


class TestTagBlockPool(unittest.TestCase):
    def test_init(self):
        pool = TagBlockPool(100, [])
        freeblocks = pool.get_blocks_of_tag(tag=TFREE)
        self.assertEqual(len(freeblocks), 100)

    def test_changing_tag(self):
        pool = TagBlockPool(100, [TDATA])

        pool.change_tag(blocknum=0, src=TFREE, dst=TDATA)
        self.assertEqual(pool.count_blocks(tag=TDATA), 1)
        self.assertEqual(pool.count_blocks(tag=TFREE), 99)

        pool.change_tag(blocknum=0, src=TDATA, dst=TFREE)
        self.assertEqual(pool.count_blocks(tag=TDATA), 0)
        self.assertEqual(pool.count_blocks(tag=TFREE), 100)

    def test_changing_to_multiple_tags(self):
        pool = TagBlockPool(100, [TDATA, TTRANS])
        pool.change_tag(blocknum=0, src=TFREE, dst=TDATA)
        pool.change_tag(blocknum=1, src=TFREE, dst=TTRANS)
        pool.change_tag(blocknum=2, src=TFREE, dst=TTRANS)

        self.assertEqual(pool.count_blocks(tag=TFREE), 97)
        self.assertEqual(pool.count_blocks(tag=TDATA), 1)
        self.assertEqual(pool.count_blocks(tag=TTRANS), 2)

    def test_pick_and_change(self):
        pool = TagBlockPool(100, [TDATA, TTRANS])
        block = pool.pick_and_move(src=TFREE, dst=TDATA)

        self.assertIn(block, pool.get_blocks_of_tag(TDATA))

    def test_pick(self):
        pool = TagBlockPool(100, [TDATA, TTRANS])
        block = pool.pick(tag=TFREE)

        self.assertIn(block, pool.get_blocks_of_tag(TFREE))
        self.assertNotIn(block, pool.get_blocks_of_tag(TDATA))
        self.assertNotIn(block, pool.get_blocks_of_tag(TTRANS))

    def test_pick_empty(self):
        pool = TagBlockPool(100, [TDATA, TTRANS])
        block = pool.pick(tag=TDATA)

        self.assertEqual(block, None)

    def test_initial_count(self):
        pool = TagBlockPool(100, [TDATA, TTRANS])

        for i in range(100):
            cnt = pool.get_erasure_count(blocknum = i)
            self.assertEqual(cnt, 0)

    def test_count_of_blocks(self):
        pool = TagBlockPool(100, [TDATA, TTRANS])

        block = pool.pick_and_move(TFREE, TDATA)

        cnt = pool.get_erasure_count(blocknum = block)
        self.assertEqual(cnt, 0)

        pool.change_tag(block, TDATA, TFREE)
        cnt = pool.get_erasure_count(blocknum = block)
        self.assertEqual(cnt, 1)

        pool.change_tag(block, TFREE, TDATA)
        pool.change_tag(block, TDATA, TFREE)
        cnt = pool.get_erasure_count(blocknum = block)
        self.assertEqual(cnt, 2)

    def test_least_erased_block(self):
        pool = TagBlockPool(10, [TDATA, TTRANS])

        # erase 0..8 twice
        for blocknum in range(9):
            pool.change_tag(blocknum, TFREE, TDATA)
            pool.change_tag(blocknum, TDATA, TFREE)

            pool.change_tag(blocknum, TFREE, TDATA)
            pool.change_tag(blocknum, TDATA, TFREE)

        least = pool.get_least_or_most_erased_block(TFREE)
        self.assertEqual(least, 9)

        least = pool.get_least_or_most_erased_block(TDATA)
        self.assertEqual(least, None)

    def test_most_erased_block(self):
        pool = TagBlockPool(10, [TDATA, TTRANS])

        # erase 0..8 twice
        for blocknum in range(9):
            pool.change_tag(blocknum, TFREE, TDATA)
            pool.change_tag(blocknum, TDATA, TFREE)

            pool.change_tag(blocknum, TFREE, TDATA)
            pool.change_tag(blocknum, TDATA, TFREE)

        most = pool.get_least_or_most_erased_block(TFREE, choice='most')
        self.assertIn(most, range(9))

        most = pool.get_least_or_most_erased_blocks(TFREE,
                choice=MOST_ERASED, nblocks=2)
        self.assertIn(most[0], range(9))
        self.assertIn(most[1], range(9))

        least = pool.get_least_or_most_erased_block(TDATA)
        self.assertEqual(least, None)

    def test_greedy_pick(self):
        pool = TagBlockPool(10, [TDATA, TTRANS])

        # erase 0..8 twice
        for blocknum in range(9):
            pool.change_tag(blocknum, TFREE, TDATA)
            pool.change_tag(blocknum, TDATA, TFREE)

            pool.change_tag(blocknum, TFREE, TDATA)
            pool.change_tag(blocknum, TDATA, TFREE)

        least_used = pool.pick_and_move(TFREE, TDATA)

        self.assertEqual(least_used, 9)

    def test_getting_count_distribution(self):
        pool = TagBlockPool(5, [TDATA, TTRANS])

        # erase 0,1 three times
        for blocknum in [0, 1]:
            for i in range(3):
                pool.change_tag(blocknum, TFREE, TDATA)
                pool.change_tag(blocknum, TDATA, TFREE)

        # erase 2,3 twice
        for blocknum in [2, 3]:
            for i in range(2):
                pool.change_tag(blocknum, TFREE, TDATA)
                pool.change_tag(blocknum, TDATA, TFREE)

        dist = pool.get_erasure_count_dist()
        # distribution:
        # key: erasure count
        # value: number of blocks
        # dist[3] = 4: 4 blocks have been erased 3 times
        self.assertEqual(dist[0], 1)
        self.assertEqual(dist[2], 2)
        self.assertEqual(dist[3], 2)
        print dist


class TestBlockPoolWithCurBlocks(unittest.TestCase):
    def test_init(self):
        tmp = BlockPoolWithCurBlocks(100, [TDATA, TTRANS], NPAGESPERBLOCK)

    def test_get_cur_block_obj(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], NPAGESPERBLOCK)
        self.assertEqual(pool.get_cur_block_obj(tag=TDATA, block_index=0), None)

    def test_next_ppns_from_cur_block(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 3)

        ppnlist = pool.next_ppns_from_cur_block(n=1, tag=TDATA, block_index=0)
        self.assertEqual(len(ppnlist), 0)

        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        pool.set_new_cur_block(TDATA, block_index=0, blocknum=new_block)
        ppnlist = pool.next_ppns_from_cur_block(n=1, tag=TDATA, block_index=0)
        self.assertEqual(len(ppnlist), 1)
        cur_block_obj = pool.get_cur_block_obj(tag=TDATA, block_index=0)
        cur_block_obj.next_page_offset = 1

    def test_get_all_cur_blocks(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 3)

        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        pool.set_new_cur_block(TDATA, block_index=0, blocknum=new_block)

        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        pool.set_new_cur_block(TDATA, block_index=1, blocknum=new_block)

        ppnlist1 = pool.next_ppns_from_cur_block(n=1, tag=TDATA, block_index=0)
        ppnlist2 = pool.next_ppns_from_cur_block(n=1, tag=TDATA, block_index=1)
        self.assertEqual(len(ppnlist1), 1)
        self.assertEqual(len(ppnlist2), 1)

        for ppn in ppnlist1:
            self.assertNotIn(ppn, ppnlist2)

        cur_block_objs = pool.get_cur_block_obj(tag=TDATA)
        self.assertEqual(len(cur_block_objs), 2)

        cur_block_objs[0].next_page_offset = 1
        cur_block_objs[1].next_page_offset = 1

    def test_next_ppns_from_cur_block_overflow(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 8)
        # will use one block
        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        pool.set_new_cur_block(TDATA, block_index=0, blocknum=new_block)
        ppnlist1 = pool.next_ppns_from_cur_block(n=8, tag=TDATA, block_index=0)
        self.assertEqual(len(ppnlist1), 8)
        self.assertEqual(pool.count_blocks(tag=TDATA), 1)
        self.assertEqual(pool.count_blocks(tag=TFREE), 99)

        ppnlist2 = pool.next_ppns_from_cur_block(n=4, tag=TDATA, block_index=0)
        self.assertEqual(len(ppnlist2), 0)

        self.assertEqual(pool.count_blocks(tag=TDATA), 1)
        self.assertEqual(pool.count_blocks(tag=TFREE), 99)

        for ppn in ppnlist1:
            self.assertNotIn(ppn, ppnlist2)

    def test_remove_full_cur_blocks(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 8)
        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        pool.set_new_cur_block(TDATA, block_index=0, blocknum=new_block)
        ppnlist1 = pool.next_ppns_from_cur_block(n=8, tag=TDATA, block_index=0)

        curblock = pool.get_cur_block_obj(tag=TDATA, block_index=0)
        self.assertEqual(curblock.is_full(), True)
        self.assertEqual(len(pool.get_cur_block_obj(tag=TDATA)), 1)

        pool.remove_full_cur_blocks()
        self.assertEqual(len(pool.get_cur_block_obj(tag=TDATA)), 0)

        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        pool.set_new_cur_block(TDATA, block_index=0, blocknum=new_block)
        ppnlist1 = pool.next_ppns_from_cur_block(n=8, tag=TDATA, block_index=0)
        self.assertEqual(len(ppnlist1), 8)
        self.assertEqual(pool.count_blocks(tag=TDATA), 2)
        self.assertEqual(pool.count_blocks(tag=TFREE), 98)

    def test_next_ppns_from_cur_block_all(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 8)

        remaining = 800

        allppns = []
        while remaining > 0:
            ppnlist = pool.next_ppns_from_cur_block(n=remaining, tag=TDATA,
                    block_index=0)
            allppns.extend(ppnlist)

            if len(ppnlist) == 0:
                new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
                pool.set_new_cur_block(TDATA, block_index=0, blocknum=new_block)

            remaining -= len(ppnlist)

        self.assertEqual(len(allppns), 800)
        self.assertEqual(pool.count_blocks(tag=TDATA), 100)
        self.assertEqual(pool.count_blocks(tag=TFREE), 0)

        new_block = pool.pick_and_move(src=TFREE, dst=TDATA)
        self.assertEqual(new_block, None)

    def test_next_ppns_context(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 8)




class TestCurrentBlock(unittest.TestCase):
    def test_init(self):
        cur_block = CurrentBlock(64, 1)

    def test_next_ppns(self):
        cur_block = CurrentBlock(64, 0)
        ppns = cur_block.next_ppns(1)
        self.assertEqual(len(ppns), 1)
        self.assertEqual(ppns[0], 0)
        self.assertEqual(cur_block.next_page_offset, 1)

    def test_next_ppns_overflow(self):
        cur_block = CurrentBlock(64, 0)
        ppns = cur_block.next_ppns(65)
        self.assertEqual(len(ppns), 64)
        self.assertEqual(cur_block.next_page_offset, 64)
        self.assertTrue(cur_block.is_full())

        ppns = cur_block.next_ppns(1)
        self.assertEqual(len(ppns), 0)

    def test_num_free(self):
        cur_block = CurrentBlock(64, 0)
        self.assertEqual(cur_block.num_free_pages(), 64)

        ppns = cur_block.next_ppns(5)
        self.assertEqual(cur_block.num_free_pages(), 59)

        ppns = cur_block.next_ppns(64)
        self.assertEqual(cur_block.num_free_pages(), 0)


def main():
    unittest.main()

if __name__ == '__main__':
    main()



