import unittest

from accpatterns import patterns
from accpatterns.patterns import READ, WRITE, DISCARD


class TestRandom(unittest.TestCase):
    def test_one(self):
        rand_iter = patterns.Random(op=READ, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)
        reqs = list(rand_iter)
        self.assertEqual(len(reqs), 1)

        req = reqs[0]
        self.assertEqual(req.op, READ)
        self.assertEqual(req.offset, 0)
        self.assertEqual(req.size, 2)

    def test_multiple(self):
        rand_iter = patterns.Random(op=READ, zone_offset=0, zone_size=20,
                chunk_size=2, traffic_size=40)
        reqs = list(rand_iter)
        self.assertEqual(len(reqs), 20)

        for req in reqs:
            self.assertEqual(req.op, READ)
            self.assertTrue(req.offset >= 0 and req.offset <= 20 - 2)
            self.assertEqual(req.size, 2)


class TestRandom(unittest.TestCase):
    def test_one(self):
        rand_iter = patterns.Sequential(op=READ, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)
        reqs = list(rand_iter)
        self.assertEqual(len(reqs), 1)

        req = reqs[0]
        self.assertEqual(req.op, READ)
        self.assertEqual(req.offset, 0)
        self.assertEqual(req.size, 2)

    def test_multiple(self):
        rand_iter = patterns.Sequential(op=READ, zone_offset=0, zone_size=10,
                chunk_size=2, traffic_size=16)
        reqs = list(rand_iter)
        self.assertEqual(len(reqs), 8)

        for i, req in enumerate(reqs):
            self.assertEqual(req.op, READ)
            self.assertTrue(req.offset >= 0 and req.offset <= 10 - 2)
            self.assertEqual(req.size, 2)
            self.assertEqual(i*2 % 10, req.offset)


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


