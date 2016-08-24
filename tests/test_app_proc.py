import unittest
import os

from workrunner.appprocess import *
from workrunner.workload import *
import ssdbox
from utilities import utils
from ssdbox.bitmap import FlashBitmap2

def create_config():
    conf = ssdbox.dftldes.Config()
    conf['SSDFramework']['ncq_depth'] = 1

    conf['flash_config']['n_pages_per_block'] = 64
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    logicsize_mb = 64
    conf.n_cache_entries = conf.n_mapping_entries_per_page
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


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


class TestAppMix(unittest.TestCase):
    def test(self):
        conf = create_config()
        mix = AppMix(conf, workload_conf_key = None)
        mix.run()



def main():
    unittest.main()

if __name__ == '__main__':
    main()





