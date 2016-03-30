import unittest

import FtlSim


class Test_lrucache(unittest.TestCase):
    def test1(self):
        lrucache = FtlSim.lrulist.LruCache()
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

def main():
    unittest.main()

if __name__ == '__main__':
    main()






