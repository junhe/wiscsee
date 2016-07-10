import unittest


from accpatterns.patterns import *
from accpatterns.contractbench import *


class TestAlignment(unittest.TestCase):
    def test_init(self):
        alignbench = Alignment(block_size=128*KB, space_size=128*MB,
                aligned=True, op=OP_WRITE)
        alignbench = Alignment(block_size=128*KB, space_size=128*MB,
                aligned=False, op=OP_READ)

    def test_aligned(self):
        alignbench = Alignment(block_size=4, space_size=12,
                aligned=True, op=OP_WRITE)

        reqs = list(alignbench)
        self.assertEqual(len(reqs), 6)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 2, 4, 6, 8, 10])

        for req in reqs:
            self.assertEqual(req.op, OP_WRITE)
            self.assertEqual(req.size, 2)

    def test_unaligned(self):
        alignbench = Alignment(block_size=4, space_size=12,
                aligned=False, op=OP_WRITE)

        reqs = list(alignbench)
        self.assertEqual(len(reqs), 6)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [2, 0, 6, 4, 10, 8])

        for req in reqs:
            self.assertEqual(req.op, OP_WRITE)
            self.assertEqual(req.size, 2)









def main():
    unittest.main()

if __name__ == '__main__':
    main()


