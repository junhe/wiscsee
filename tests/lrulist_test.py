import unittest

import wiscsim
from wiscsim.lrulist import LinkedList, Node, LruDict, LruCache
import profile


class Test_lrucache(unittest.TestCase):
    def test1(self):
        lrucache = wiscsim.lrulist.LruCache()
        lrucache[1] = 11
        lrucache[2] = 22
        lrucache[3] = 33

        self.assertEqual(lrucache.least_recently_used_key(), 1)
        self.assertEqual(lrucache.most_recently_used_key(), 3)
        lrucache[2] = 222
        self.assertEqual(lrucache.most_recently_used_key(), 2)
        self.assertEqual(lrucache.peek(1), 11)
        # peek should not change order
        self.assertEqual(lrucache.most_recently_used_key(), 2)

        lrucache.orderless_update(1, 111)
        self.assertEqual(lrucache.most_recently_used_key(), 2)
        self.assertEqual(lrucache.peek(1), 111)

        del lrucache[1]
        self.assertEqual(lrucache.has_key(1), False)

class Test_LruCache(unittest.TestCase):
    def get_lrucache(self):
        d = LruCache()
        for i in range(10):
            d[i] = i*10
        return d

    def test_init(self):
        d = LruCache()
        d = LruCache({1:2})
        d = LruCache(((1, 2), (2, 3)))
        d = LruCache(a = 1, b = 2)

    def test1(self):
        d = self.get_lrucache()

    def test_size(self):
        d = self.get_lrucache()
        self.assertEqual(len(d), 10)

    def test_del(self):
        d = self.get_lrucache()

        del d[2]
        self.assertEqual(len(d), 9)
        self.assertEqual(d.has_key(2), False)

    def test_iter(self):
        d = self.get_lrucache()
        self.assertListEqual(list(d), list(reversed(range(10))))

    def test_reversed(self):
        d = self.get_lrucache()
        self.assertListEqual(list(reversed(d)), list(range(10)))

    def test_items(self):
        d = self.get_lrucache()
        lk = []
        lv = []
        for k, v in d.items():
            # suppose to go from least to most recently
            lk.append(k)
            lv.append(v)
        self.assertListEqual(lk, list(range(10)))
        self.assertListEqual(lv, list(range(0, 100, 10)))

    def test_recency_iter(self):
        d = LruDict()
        d[1] = 11
        d[2] = 22

        l = []
        for k in d.least_to_most_iter():
            l.append(k)
        self.assertListEqual(l, [1, 2])

        l = []
        for k in d.most_to_least_iter():
            l.append(k)
        self.assertListEqual(l, [2, 1])

    def test_hits(self):
        d = self.get_lrucache()
        a = d[2]

        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recently_used_key(), 2)

        d[2] = 22
        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recently_used_key(), 2)

        d[3] = 333
        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recently_used_key(), 3)

    def test_peek(self):
        d = self.get_lrucache()

        a = d.peek(2)
        self.assertEqual(a, 20)
        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recently_used_key(), 9)

    def test_add_to_least_used(self):
        d = self.get_lrucache()

        d.add_as_least_used(10, 100)
        self.assertEqual(d.victim_key(), 10)
        self.assertEqual(d.most_recently_used_key(), 9)

    def _test_performance(self):
        d = LruDict()
        for i in range(2048):
            d[i] = i+1


    def go_through(self, d):
        for k, v in d.least_to_most_items():
            v = 1




class Test_LruDict(unittest.TestCase):
    def get_lrudict(self):
        d = LruDict()
        for i in range(10):
            d[i] = i*10
        return d

    def test_init(self):
        d = LruDict()
        d = LruDict({1:2})
        d = LruDict(((1, 2), (2, 3)))
        d = LruDict(a = 1, b = 2)

    def test1(self):
        d = self.get_lrudict()

    def test_size(self):
        d = self.get_lrudict()
        self.assertEqual(len(d), 10)

    def test_del(self):
        d = self.get_lrudict()

        del d[2]
        self.assertEqual(len(d), 9)
        self.assertEqual(d.has_key(2), False)

    def test_iter(self):
        d = self.get_lrudict()
        self.assertListEqual(list(d), list(range(10)))

    def test_revsersed(self):
        d = self.get_lrudict()
        self.assertListEqual(list(reversed(d)), list(reversed(range(10))))

    def test_items(self):
        d = self.get_lrudict()
        lk = []
        lv = []
        for k, v in d.items():
            lk.append(k)
            lv.append(v)
        self.assertListEqual(lk, list(range(10)))
        self.assertListEqual(lv, list(range(0, 100, 10)))

    def test_recency_iter(self):
        d = LruDict()
        d[1] = 11
        d[2] = 22

        l = []
        for k in d.least_to_most_iter():
            l.append(k)
        self.assertListEqual(l, [1, 2])

        l = []
        for k in d.most_to_least_iter():
            l.append(k)
        self.assertListEqual(l, [2, 1])

    def test_hits(self):
        d = self.get_lrudict()
        a = d[2]

        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recent(), 2)

        d[2] = 22
        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recent(), 2)

        d[3] = 333
        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recent(), 3)

    def test_peek(self):
        d = self.get_lrudict()

        a = d.peek(2)
        self.assertEqual(a, 20)
        self.assertEqual(d.victim_key(), 0)
        self.assertEqual(d.most_recent(), 9)

    def _test_performance(self):
        d = LruDict()
        for i in range(2048):
            d[i] = i+1

        self.go_through(d)

    def go_through(self, d):
        for k, v in d.least_to_most_items():
            v != 1


def has_key(d, key):
    return d.has_key(key)

def compare_dict_performance():
    # d = LruCache()
    d = dict()
    for i in range(40000):
        d[i] = i

    for i in range(200000):
        # d.has_key(i)
        has_key(d, i)

def main():
    unittest.main()



if __name__ == '__main__':
    main()


