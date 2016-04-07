import unittest

import ssdbox
from ssdbox.lrulist import LinkedList, Node, LruDict


class Test_lrucache(unittest.TestCase):
    def test1(self):
        lrucache = ssdbox.lrulist.LruCache()
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


def main():
    unittest.main()

if __name__ == '__main__':
    main()






