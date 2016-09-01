import unittest
import time
import os

from workrunner.appprocess import *
from workrunner.workload import *
import ssdbox
from utilities import utils
from ssdbox.bitmap import FlashBitmap2

class TestLevelDB(unittest.TestCase):
    def test_leveldb(self):
        leveldb_proc = LevelDBProc(
            benchmarks='overwrite',
            num=1000,
            db='/tmp/leveldbtest',
            threads=1,
            use_existing_db=0,
            max_key=200,
            max_log=-1)
        leveldb_proc.run()
        leveldb_proc.wait()

    @unittest.skip('too long')
    def test_leveldb_kill(self):
        leveldb_proc = LevelDBProc(
            benchmarks='overwrite',
            num=10000000,
            db='/tmp/leveldbtest',
            threads=1,
            use_existing_db=0,
            max_key=2000000,
            max_log=-1)
        leveldb_proc.run()
        print 'sleeping...'
        time.sleep(3)
        print 'wake up...'
        leveldb_proc.terminate()
        # leveldb_proc.p.kill()
        print 'terminated'
        while True:
            time.sleep(1)
            utils.shcmd('ps -ef|grep db_bench')


class TestSqlite(unittest.TestCase):
    def test_sqlite(self):
        sqlite_proc = SqliteProc(
                n_insertions = 100,
                pattern = 'random',
                db_dir = '/tmp/sqlite_tmp_xlj23',
                commit_period = 10,
                max_key = 20
                )
        sqlite_proc.run()
        sqlite_proc.wait()

    @unittest.skip('too long')
    def test_sqlite_kill(self):
        sqlite_proc = SqliteProc(
                n_insertions = 100000,
                pattern = 'random',
                db_dir = '/tmp/sqlite_tmp_xlj23',
                commit_period = 10,
                max_key = 20
                )
        sqlite_proc.run()

        print 'sleeping'
        time.sleep(3)
        utils.shcmd('ps -ef|grep bench.py')
        sqlite_proc.terminate()
        print 'after.......'

        while True:
            time.sleep(1)
            utils.shcmd('ps -ef|grep bench.py')

class TestVarmail(unittest.TestCase):
    def test_varmail(self):
        proc = VarmailProc('/tmp/varmail_tmp_lj23lj', 1)
        proc.run()
        proc.wait()

    @unittest.skip('too long')
    def test_varmail_kill(self):
        proc = VarmailProc('/tmp/varmail_tmp_lj23lj', 191)
        proc.run()
        time.sleep(5)
        proc.terminate()

        while True:
            time.sleep(1)
            utils.shcmd('ps -ef|grep filebench')

def main():
    unittest.main()

if __name__ == '__main__':
    main()





