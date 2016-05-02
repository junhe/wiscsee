import unittest

from accpatterns import patterns
from accpatterns.patterns import READ, WRITE, DISCARD


class TestRandom(unittest.TestCase):
    def test_one(self):
        pat_iter = patterns.Random(op=READ, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 1)

        req = reqs[0]
        self.assertEqual(req.op, READ)
        self.assertEqual(req.offset, 0)
        self.assertEqual(req.size, 2)

    def test_multiple(self):
        pat_iter = patterns.Random(op=READ, zone_offset=0, zone_size=20,
                chunk_size=2, traffic_size=40)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 20)

        for req in reqs:
            self.assertEqual(req.op, READ)
            self.assertTrue(req.offset >= 0 and req.offset <= 20 - 2)
            self.assertEqual(req.size, 2)


class TestSequential(unittest.TestCase):
    def test_one(self):
        pat_iter = patterns.Sequential(op=READ, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 1)

        req = reqs[0]
        self.assertEqual(req.op, READ)
        self.assertEqual(req.offset, 0)
        self.assertEqual(req.size, 2)

    def test_multiple(self):
        pat_iter = patterns.Sequential(op=READ, zone_offset=0, zone_size=10,
                chunk_size=2, traffic_size=16)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 8)

        for i, req in enumerate(reqs):
            self.assertEqual(req.op, READ)
            self.assertTrue(req.offset >= 0 and req.offset <= 10 - 2)
            self.assertEqual(req.size, 2)
            self.assertEqual(i*2 % 10, req.offset)


class TestHotNCold(unittest.TestCase):
    def test_first_pass(self):
        pat_iter = patterns.HotNCold(op=WRITE, zone_offset=0, zone_size=10,
                chunk_size=2, traffic_size=10)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 5)

        for i, req in enumerate(reqs):
            self.assertEqual(req.op, WRITE)
            self.assertTrue(req.offset >= 0 and req.offset <= 10 - 2)
            self.assertEqual(req.size, 2)
            self.assertEqual(i*2 % 10, req.offset)

    def test_more_passes(self):
        pat_iter = patterns.HotNCold(op=WRITE, zone_offset=0, zone_size=4,
                chunk_size=1, traffic_size=8)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 8)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 1, 2, 3, 0, 2, 0, 2])


class TestStrided(unittest.TestCase):
    def test_basic(self):
        pat_iter = patterns.Strided(op=WRITE, zone_offset=0, zone_size=10,
                chunk_size=2, traffic_size=6, stride_size=5)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 3)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs, [0, 5, 0])


class TestSnake(unittest.TestCase):
    def test_basic(self):
        pat_iter = patterns.Snake(zone_offset=0, zone_size=10,
                chunk_size=2, traffic_size=20, snake_size=4)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 18)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs,
            [0, 2,  4, 0,  6, 2,  8, 4,  0, 6,  2, 8,  4, 0,  6, 2,  8, 4])

        ops = [req.op for req in reqs]
        self.assertListEqual(ops,
                [WRITE, WRITE, WRITE, DISCARD, WRITE,
                DISCARD, WRITE, DISCARD, WRITE, DISCARD,
                WRITE, DISCARD, WRITE, DISCARD,
                WRITE, DISCARD, WRITE, DISCARD
                ])


class TestFadingSnake(unittest.TestCase):
    def test_basic(self):
        pat_iter = patterns.FadingSnake(zone_offset=0, zone_size=10,
                chunk_size=2, traffic_size=20, snake_size=4)
        reqs = list(pat_iter)
        self.assertEqual(len(reqs), 18)

        offs = [req.offset for req in reqs]
        self.assertListEqual(offs[:3], [0, 2, 4])

        ops = [req.op for req in reqs]
        self.assertListEqual(ops,
                [WRITE, WRITE, WRITE, DISCARD, WRITE,
                DISCARD, WRITE, DISCARD, WRITE, DISCARD,
                WRITE, DISCARD, WRITE, DISCARD,
                WRITE, DISCARD, WRITE, DISCARD
                ])

        valid_offs = []
        for req in reqs:
            if req.op == WRITE:
                valid_offs.append(req.offset)
            elif req.op == DISCARD:
                self.assertIn(req.offset, valid_offs)
                valid_offs.remove(req.offset)


class TestMix(unittest.TestCase):
    def test_basic(self):
        a = [0, 1, 2]
        cpy = list(patterns.mix(iter(a)))
        self.assertListEqual(list(a), list(cpy))

    def test_basic2(self):
        a = [0]
        cpy = list(patterns.mix(iter(a)))
        self.assertListEqual(a, cpy)

    def test_basic3(self):
        a = []
        cpy = list(patterns.mix(iter(a)))
        self.assertListEqual(a, cpy)

    def test_mixing_two(self):
        a = [0, 1]
        b = [4, 5]
        c = list(patterns.mix(iter(a), iter(b)))
        self.assertListEqual(list(c), [0, 4, 1, 5])

    def test_mixing_two_2(self):
        a = [0]
        b = [4, 5]
        c = list(patterns.mix(iter(a), iter(b)))
        self.assertListEqual(list(c), [0, 4, 5])

    def test_mixing_two_3(self):
        a = []
        b = [4, 5]
        c = list(patterns.mix(iter(a), iter(b)))
        self.assertListEqual(list(c), [4, 5])

    def test_mixing_three(self):
        a = [0, 1]
        b = [2, ]
        c = [4, 5]
        d = list(patterns.mix(iter(a), iter(b), iter(c)))
        self.assertListEqual(list(d), [0, 2, 4, 1, 5])

def main():
    unittest.main()

if __name__ == '__main__':
    main()


