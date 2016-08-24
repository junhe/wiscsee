import unittest
import os

from workrunner.appprocess import *


class Test(unittest.TestCase):
    def test_leveldb(self):
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

    def test_sqlite(self):
        sqlite_proc = SqliteProc(
                n_insertions = 100,
                pattern = 'random',
                db_dir = '/tmp/sqlite_tmp_xlj23')
        sqlite_proc.run()
        sqlite_proc.wait()

    def test_varmail(self):
        proc = VarmailProc('/tmp/varmail_tmp_lj23lj', 1)
        proc.run()
        proc.wait()


def main():
    unittest.main()

if __name__ == '__main__':
    main()





