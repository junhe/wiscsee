import unittest


from accpatterns.patterns import *
from accpatterns.contractbench import *


class TestAlignment(unittest.TestCase):
    def test_init(self):
        alignbench = Alignment(block_size=128*KB, traffic_size=128*MB,
                aligned=True, op=OP_WRITE)
        alignbench = Alignment(block_size=128*KB, traffic_size=128*MB,
                aligned=False, op=OP_READ)

    def test_aligned(self):
        alignbench = Alignment(block_size=4, traffic_size=12,
                aligned=True, op=OP_WRITE)

        reqs = list(alignbench)
        self.assertEqual(len(reqs), 6)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 2, 4, 6, 8, 10])

        for req in reqs:
            self.assertEqual(req.op, OP_WRITE)
            self.assertEqual(req.size, 2)

    def test_unaligned(self):
        alignbench = Alignment(block_size=4, traffic_size=12,
                aligned=False, op=OP_WRITE)

        reqs = list(alignbench)
        self.assertEqual(len(reqs), 6)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [2, 0, 6, 4, 10, 8])

        for req in reqs:
            self.assertEqual(req.op, OP_WRITE)
            self.assertEqual(req.size, 2)


class TestRequestScale(unittest.TestCase):
    def test_init(self):
        reqbench = RequestScale(space_size=128*MB, chunk_size=4*KB,
                traffic_size=8*MB, op=OP_WRITE, n_ncq_slots=16)

    def test_scale_write(self):
        bench = RequestScale(space_size=10, chunk_size=2,
                traffic_size=10, op=OP_WRITE, n_ncq_slots=16)

        reqs = [req for req in list(bench) if isinstance(req, Request)]

        offs = [req.offset for req in reqs]
        ops = [req.op for req in reqs]

        self.assertEqual(len(reqs), 7)
        self.assertListEqual(ops, [OP_WRITE] * 5 + [OP_DISCARD] * 2)

        for req in reqs:
            self.assertEqual(req.size, 2)

    def test_scale_read(self):
        bench = RequestScale(space_size=10, chunk_size=2,
                traffic_size=20, op=OP_READ, n_ncq_slots=16)

        reqs = [req for req in list(bench) if isinstance(req, Request)]

        offs = [req.offset for req in reqs]

        req0 = reqs[0]
        self.assertEqual(req0.offset, 0)
        self.assertEqual(req0.size, 10)
        self.assertEqual(req0.op, OP_WRITE)

        self.assertEqual(len(reqs), 11)

        for req in reqs[1:]:
            self.assertEqual(req.op, OP_READ)
            self.assertEqual(req.size, 2)


class TestGroupByInvTimeAtAccTime(unittest.TestCase):
    def test_init(self):
        bench = GroupByInvTimeAtAccTime(space_size=128*MB, traffic_size=128*MB,
                chunk_size=4*KB, grouping=True, n_ncq_slots=16)

    def test_group_by_time(self):
        bench = GroupByInvTimeAtAccTime(space_size=32, traffic_size=4,
                chunk_size=2, grouping=True, n_ncq_slots=16)

        reqs = [req for req in list(bench) if isinstance(req, Request)]
        self.assertEqual(len(reqs), 6)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 4, 2, 6, 0, 4])

        ops = [req.op for req in reqs]
        self.assertListEqual(ops, [OP_WRITE, OP_WRITE, OP_WRITE, OP_WRITE,
            OP_DISCARD, OP_DISCARD])

        for req in reqs:
            self.assertEqual(req.size, 2)

    def test_not_group_by_time(self):
        bench = GroupByInvTimeAtAccTime(space_size=32, traffic_size=4,
                chunk_size=2, grouping=False, n_ncq_slots=16)

        reqs = [req for req in list(bench) if isinstance(req, Request)]
        self.assertEqual(len(reqs), 6)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 2, 4, 6, 0, 4])

        ops = [req.op for req in reqs]
        self.assertListEqual(ops, [OP_WRITE, OP_WRITE, OP_WRITE, OP_WRITE,
            OP_DISCARD, OP_DISCARD])

        for req in reqs:
            self.assertEqual(req.size, 2)


class TestGroupInvTimeInSpace(unittest.TestCase):
    def test_init(self):
        bench = GroupByInvTimeInSpace(space_size=128*MB, traffic_size=128*MB,
                chunk_size=4*KB, grouping=True, n_ncq_slots=16)

    def test_group_at_space(self):
        bench = GroupByInvTimeInSpace(space_size=32, traffic_size=4,
                chunk_size=2, grouping=True, n_ncq_slots=16)

        reqs = [req for req in list(bench) if isinstance(req, Request)]
        self.assertEqual(len(reqs), 6)

        # 0 2 4 6
        # A A B B
        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 4, 2, 6, 0, 2])

        ops = [req.op for req in reqs]
        self.assertListEqual(ops, [OP_WRITE, OP_WRITE, OP_WRITE, OP_WRITE,
            OP_DISCARD, OP_DISCARD])

        for req in reqs:
            self.assertEqual(req.size, 2)

    def test_not_group_at_space(self):
        bench = GroupByInvTimeInSpace(space_size=32, traffic_size=4,
                chunk_size=2, grouping=False, n_ncq_slots=16)

        reqs = [req for req in list(bench) if isinstance(req, Request)]
        self.assertEqual(len(reqs), 6)

        # 0 2 4 6
        # A B A B
        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 2, 4, 6, 0, 4])

        ops = [req.op for req in reqs]
        self.assertListEqual(ops, [OP_WRITE, OP_WRITE, OP_WRITE, OP_WRITE,
            OP_DISCARD, OP_DISCARD])

        for req in reqs:
            self.assertEqual(req.size, 2)



def main():
    unittest.main()

if __name__ == '__main__':
    main()


