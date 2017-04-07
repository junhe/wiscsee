import unittest

from benchmarks.expconfs import *


class TestParameterPool(unittest.TestCase):
    def test_init(self):
        pool = ParameterPool(
                expname = 'myexpname',
                testname = ['sqliteWAL_reqscale_w_rand'],
                filesystem = ['ext4', 'f2fs']
                )



def main():
    unittest.main()

if __name__ == '__main__':
    main()



