import unittest

import FtlSim

class Test_datacache(unittest.TestCase):
    def test1(self):
        datacache = FtlSim.datacache.DataCache(max_n_entries = 5)
        datacache.add_entry(lpn = 3, data = 33, dirty = False)
        self.assertEqual(datacache.get_entry(3), (33, False))
        datacache.update_entry(lpn = 3, data = 333, dirty = True)
        self.assertEqual(datacache.get_entry(3), (333, True))


def main():
    unittest.main()

if __name__ == '__main__':
    main()






