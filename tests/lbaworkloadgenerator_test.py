import unittest

from Makefile import *

class TestMultipleProcess(unittest.TestCase):
    def test_main(self):
        conf = ssdbox.dftldes.Config()

        w = 'write'
        r = 'read'
        d = 'discard'
        conf["lba_workload_configs"]["MultipleProcess"] = {
                "events": [
                    [(d, 1, 3),
                    (w, 1, 3),
                    (d, 1, 3)],

                    [(d, 2, 3),
                    (w, 3, 3),
                    (d, 4, 3)]
                    ]}

        lbagen = workrunner.lbaworkloadgenerator.MultipleProcess(conf)
        iter_list = lbagen.get_iter_list()
        self.assertTrue(isinstance(iter_list, list))

        self.assertEqual(len(iter_list), 2)

        iter1 = list(iter_list[0])
        self.assertEqual(iter1[-1].operation, 'discard')



def main():
    unittest.main()

if __name__ == '__main__':
    main()

