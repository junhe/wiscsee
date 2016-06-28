import unittest
from ssdbox.tagblockpool import *

TDATA = 'TDATA'
TTRANS = 'TTRANS'
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

NPAGESPERBLOCK = 64
class TestBlockPoolWithCurBlocks(unittest.TestCase):
    def test_init(self):
        tmp = BlockPoolWithCurBlocks(100, [TDATA, TTRANS], NPAGESPERBLOCK)

    def test_cur_block(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], NPAGESPERBLOCK)
        self.assertEqual(pool.cur_block(tag=TDATA, block_index=0), None)

    def test_next_pages(self):
        pool = BlockPoolWithCurBlocks(100, [TDATA], 3)
        ppnlist = pool.next_pages(n=1, tag=TDATA, block_index=0)
        self.assertEqual(len(ppnlist), 1)
        cur_block_obj = pool.cur_block_obj(tag=TDATA, block_index=0)
        cur_block_obj.next_page_offset = 1

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

        ppns = cur_block.next_ppns(1)
        self.assertEqual(len(ppns), 0)



def main():
    unittest.main()

if __name__ == '__main__':
    main()



