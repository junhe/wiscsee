import unittest

from benchmarks.expconfs import *


class TestParameterPool(unittest.TestCase):
    def test_init(self):
        pool = ParameterPool(
                expname = 'myexpname',
                testname = 'rocksdb-reqscale-r-seq',
                filesystem = ['ext4', 'f2fs']
                )






def main():
    unittest.main()

if __name__ == '__main__':
    main()



