import unittest
from workrunner.appprocess import *


class TestLevelDBProc(unittest.TestCase):
    def test_init(self):
        leveldb_proc = LevelDBProc(
            benchmarks='overwrite',
            num=1000,
            db='/tmp/leveldbtest',
            outputpath='/tmp/tmpleveldbout',
            threads=1,
            use_existing_db=0,
            max_key=200,
            max_log=-1)
        leveldb_proc.run()
        leveldb_proc.wait()


def main():
    unittest.main()

if __name__ == '__main__':
    main()





