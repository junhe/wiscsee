import unittest
import random

from ssdbox.nkftl2 import Nkftl, LogGroupInfo, GlobalHelper, \
        ERR_NEED_NEW_BLOCK, ERR_NEED_MERGING, BlockPool
import ssdbox
import config
from commons import *
from utilities import utils

def create_config():
    conf = ssdbox.nkftl2.Config()

    conf['flash_config']['n_pages_per_block'] = 8
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    conf['nkftl']['max_blocks_in_log_group'] = 2
    conf['nkftl']['n_blocks_in_data_group'] = 4

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    conf['ftl_type'] = 'nkftl'
    conf['simulator_class'] = 'SimulatorDESSync'

    logicsize_mb = 64
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


def create_recorder(conf):
    rec = ssdbox.recorder.Recorder(output_target = conf['output_target'],
        output_directory = conf['result_dir'],
        verbose_level = conf['verbose_level'],
        print_when_finished = conf['print_when_finished']
        )
    rec.disable()
    return rec


def create_nkftl():
    conf = create_config()
    rec = create_recorder(conf)

    ftl = Nkftl(conf, rec,
        ssdbox.flash.Flash(recorder=rec, confobj=conf))
    return ftl, conf, rec

def create_global_helper(conf):
    return GlobalHelper(conf)

def create_loggroupinfo(conf, rec, globalhelper):
    return LogGroupInfo(conf, rec, globalhelper)

class TestNkftl(unittest.TestCase):
    def test_init(self):
        ftl, conf, rec = create_nkftl()

    def test_write_and_read(self):
        ftl, conf, rec = create_nkftl()

        ftl.lba_write(8, data='3')
        ret = ftl.lba_read(8)
        self.assertEqual(ret, '3')

    def randomdata(self, lpn):
        return str(random.randint(0, 100))

    def write_and_check(self, ftl, lpns):
        data_mirror = {}
        for lpn in lpns:
            data = self.randomdata(lpn)
            data_mirror[lpn] = data
            ftl.lba_write(lpn, data)

        for lpn, data in data_mirror.items():
            ret = ftl.lba_read(lpn)
            self.assertEqual(ret, data)

    def test_data_integrity(self):
        ftl, conf, rec = create_nkftl()

        total_pages = conf.total_num_pages()
        lpns = random.sample(range(total_pages), 1000)

        self.write_and_check(ftl, lpns)

    def test_GC_simple(self):
        ftl, conf, rec = create_nkftl()

        lpns = [0] * 4 * conf.n_pages_per_block * conf['nkftl']['max_blocks_in_log_group']
        self.write_and_check(ftl, lpns)

    def test_GC_harder(self):
        ftl, conf, rec = create_nkftl()

        lpns = [0, 3, 1] * 4 * conf.n_pages_per_block * conf['nkftl']['max_blocks_in_log_group']
        self.write_and_check(ftl, lpns)

    def test_GC_harder2(self):
        ftl, conf, rec = create_nkftl()

        lpns = [0, 128, 3, 129, 1] * 4 * conf.n_pages_per_block * conf['nkftl']['max_blocks_in_log_group']
        self.write_and_check(ftl, lpns)

    # @unittest.skipUnless(TESTALL == True, "Skip unless we want to test all")
    def test_GC_harder_super(self):
        ftl, conf, rec = create_nkftl()

        print 'total pages', conf.total_num_pages()
        lpns = [0, 128, 3, 129, 1] * 4 * conf.total_num_pages()
        self.write_and_check(ftl, lpns)


class TestLogGroupInfo(unittest.TestCase):
    def test_add_log_blocks(self):
        ftl, conf, rec = create_nkftl()
        globalhelper = create_global_helper(conf)
        loggroupinfo = create_loggroupinfo(conf, rec, globalhelper)

        loggroupinfo.add_log_block(8)

        log_blocks = loggroupinfo.log_blocks()
        self.assertEqual(log_blocks.keys()[0], 8)

    def test_next_ppn(self):
        ftl, conf, rec = create_nkftl()
        globalhelper = create_global_helper(conf)
        loggroupinfo = create_loggroupinfo(conf, rec, globalhelper)

        loggroupinfo.add_log_block(0)
        for i in range(conf.n_pages_per_block):
            found, err = loggroupinfo.next_ppn_to_program()
            self.assertTrue(found)

        found, err = loggroupinfo.next_ppn_to_program()
        self.assertFalse(found)
        self.assertEqual(err, ERR_NEED_NEW_BLOCK)

        loggroupinfo.add_log_block(1)
        for i in range(conf.n_pages_per_block):
            found, err = loggroupinfo.next_ppn_to_program()
            self.assertTrue(found)

        found, err = loggroupinfo.next_ppn_to_program()
        self.assertFalse(found)
        self.assertEqual(err, ERR_NEED_MERGING)

class TestBlockPool(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        block_pool = BlockPool(conf)

        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_log_blocks(self):
        conf = create_config()
        block_pool = BlockPool(conf)

        blocknum = block_pool.pop_a_free_block_to_log_blocks()
        self.assertIn(blocknum, block_pool.log_usedblocks)
        self.assertNotIn(blocknum, block_pool.data_usedblocks)

        block_pool.move_used_log_to_data_block(blocknum)
        self.assertIn(blocknum, block_pool.data_usedblocks)
        self.assertNotIn(blocknum, block_pool.log_usedblocks)

        block_pool.free_used_data_block(blocknum)
        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_data_blocks(self):
        conf = create_config()
        block_pool = BlockPool(conf)

        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        self.assertIn(blocknum, block_pool.data_usedblocks)
        self.assertNotIn(blocknum, block_pool.log_usedblocks)

        block_pool.free_used_data_block(blocknum)
        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_free_used_log(self):
        conf = create_config()
        block_pool = BlockPool(conf)

        blocknum = block_pool.pop_a_free_block_to_log_blocks()
        block_pool.free_used_log_block(blocknum)
        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_freeblocks(self):
        conf = create_config()
        block_pool = BlockPool(conf)

        self.assertEqual(len(block_pool.freeblocks), conf.n_blocks_per_dev)


def main():
    unittest.main()

if __name__ == '__main__':
    main()


