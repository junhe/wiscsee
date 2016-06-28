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

    def test_cur_block(self):
        pass


def main():
    unittest.main()

if __name__ == '__main__':
    main()



