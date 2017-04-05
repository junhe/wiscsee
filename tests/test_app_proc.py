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
            inst_id=0,
            do_strace=False,
            mem_limit_in_bytes=128*1024*1024
            )
        leveldb_proc.run()
        leveldb_proc.wait()

class TestSqlite(unittest.TestCase):
    def test_sqlite(self):
        sqlite_proc = SqliteProc(
                n_insertions = 100,
                pattern = 'random',
                db_dir = '/tmp/sqlite_tmp_xlj23',
                commit_period = 10,
                max_key = 20,
                inst_id=0,
                do_strace=False,
                journal_mode="DELETE",
                mem_limit_in_bytes=100*1024*1024
                )
        sqlite_proc.run()
        sqlite_proc.wait()

class TestVarmail(unittest.TestCase):
    def test_varmail(self):
        proc = VarmailProc(
                dirpath='/tmp/varmail_tmp_lj23lj',
                seconds=1,
                nfiles=8,
                num_bytes=1024,
                inst_id=1,
                do_strace=False,
                rwmode='read',
                mem_limit_in_bytes=10*1024*1024
                )
        proc.run()
        proc.wait()

    @unittest.skip('too long')
    def test_varmail_kill(self):
        proc = VarmailProc('/tmp/varmail_tmp_lj23lj', 191, 8)
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





