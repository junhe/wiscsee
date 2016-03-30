import unittest

import ssdbox

class Test_datacache(unittest.TestCase):
    def test1(self):
        datacache = ssdbox.datacache.DataCache(max_n_entries = 5, simpy_env = None)
        datacache.add_entry(lpn = 3, data = 33, dirty = False)
        self.assertEqual(datacache.get_entry(3).values(), (33, False))
        datacache.update_entry(lpn = 3, data = 333, dirty = True)
        self.assertEqual(datacache.get_entry(3).values(), (333, True))

    def test_split(self):
        datacache = ssdbox.datacache.DataCache(max_n_entries = 5, simpy_env = None)
        datacache.add_entry(lpn = 3, data = 33, dirty = False)

        subextents = datacache.split_extent(0, 5)
        self.assertEqual(len(subextents), 3)

    def test_victims(self):
        datacache = ssdbox.datacache.DataCache(max_n_entries = 5, simpy_env = None)
        for i in range(10):
            datacache.add_entry(lpn = i, data = i * 10, dirty = False)

        lpns = [lpn for lpn, entry in datacache.evict_n_entries(3)]
        self.assertListEqual(sorted(lpns), [0, 1, 2])

def main():
    unittest.main()

if __name__ == '__main__':
    main()






