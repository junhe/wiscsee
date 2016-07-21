import unittest
import random

from ssdbox.nkftl2 import *
from ssdbox import flash
import ssdbox
import config
from commons import *
from utilities import utils
from utilities.utils import choose_exp_metadata, runtime_update
from workflow import run_workflow
from config import LBAGENERATOR

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

    ftl = Ftl(conf, rec,
        ssdbox.flash.Flash(recorder=rec, confobj=conf))
    return ftl, conf, rec

def create_global_helper(conf):
    return GlobalHelper(conf)

def create_loggroupinfo(conf, rec, globalhelper):
    return LogGroupInfo(conf, rec, globalhelper)

def create_translator(conf, rec, globalhelper, log_mapping, data_block_mapping):
    return Translator(conf, rec, globalhelper, log_mapping, data_block_mapping)

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

    @unittest.skipUnless(TESTALL == True, "Skip unless we want to test all")
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

        n_pages_per_block = conf.n_pages_per_block

        log_blocks = loggroupinfo.log_blocks()
        self.assertEqual(log_blocks.keys()[0], 8)
        singlelogblockinfo = log_blocks.values()[0]
        self.assertTrue(isinstance(singlelogblockinfo, SingleLogBlockInfo))
        self.assertEqual(singlelogblockinfo.flash_block_num, 8)
        self.assertEqual(singlelogblockinfo.has_free_page(), True)
        gotit, ppn, = singlelogblockinfo.next_ppn_to_program()
        self.assertEqual(gotit, True)
        self.assertEqual(ppn, 8*n_pages_per_block)

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

    def test_init(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        lginfo = LogGroupInfo(conf, rec, helper)

    def test_add_mapping(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        lginfo = LogGroupInfo(conf, rec, helper)
        lginfo.add_log_block(1)

        lpns = []
        ppns = []
        for i in range(conf.n_pages_per_block):
            found, ppn = lginfo.next_ppn_to_program()
            self.assertTrue(found)
            lginfo.add_mapping(lpn=i, ppn=ppn)
            lpns.append(i)
            ppns.append(ppn)

        for lpn, ppn in zip(lpns, ppns):
            self.assertEqual(lginfo.lpn_to_ppn(lpn)[1], ppn)

    def test_remove_log_block(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        lginfo = LogGroupInfo(conf, rec, helper)
        lginfo.add_log_block(1)

        lpns = []
        ppns = []
        for i in range(conf.n_pages_per_block):
            found, ppn = lginfo.next_ppn_to_program()
            self.assertTrue(found)
            lginfo.add_mapping(lpn=i, ppn=ppn)
            lpns.append(i)
            ppns.append(ppn)

        lginfo.remove_log_block(1)

        self.assertEqual(len(lginfo._page_map), 0)
        self.assertEqual(len(lginfo._log_blocks), 0)
        self.assertEqual(lginfo._cur_log_block, None)

    def test_adding_log_blocks(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        lginfo = LogGroupInfo(conf, rec, helper)
        lginfo.add_log_block(1)

        with self.assertRaisesRegexp(RuntimeError, 'should not have free page'):
            lginfo.add_log_block(2)

    def test_max_log_blocks(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        max_n_blocks = conf['nkftl']['max_blocks_in_log_group']

        lginfo = LogGroupInfo(conf, rec, helper)

        for blocknum in range(max_n_blocks):
            lginfo.add_log_block(blocknum)
            for page in range(conf.n_pages_per_block):
                found, ppn = lginfo.next_ppn_to_program()
                self.assertTrue(found)
                lginfo.add_mapping(lpn=blocknum*conf.n_pages_per_block+page,
                        ppn=ppn)
            found, err = lginfo.next_ppn_to_program()
            self.assertEqual(found, False)

            if blocknum == max_n_blocks - 1:
                self.assertEqual(err, ERR_NEED_MERGING)
            else:
                self.assertEqual(err, ERR_NEED_NEW_BLOCK)


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


@unittest.skip("Failed?")
class TestWithSimulator(unittest.TestCase):
    def setup_config(self):
        self.conf = ssdbox.nkftl2.Config()
        self.conf.n_channels_per_dev = 4

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkload"
        self.conf["lba_workload_configs"]["ExtentTestWorkload"] = {
            "op_count": 1000}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'nkftl2'
        self.conf['simulator_class'] = 'SimulatorNonDESe2eExtent'

        logicsize_mb = 16
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def my_run(self):
        runtime_update(self.conf)
        run_workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestBlockInfo(unittest.TestCase):
    def test_init(self):
        blkinfo = BlockInfo(block_type=TYPE_DATA_BLOCK,
                block_num=23, last_used_time=88, valid_ratio=0.8,
                data_group_no=8)

    def test_comp(self):
        blkinfo1 = BlockInfo(block_type=TYPE_DATA_BLOCK,
                block_num=23, last_used_time=88, valid_ratio=0.8,
                data_group_no=8)

        blkinfo2 = BlockInfo(block_type=TYPE_DATA_BLOCK,
                block_num=23, last_used_time=89, valid_ratio=0.8,
                data_group_no=8)

        blkinfo3 = BlockInfo(block_type=TYPE_DATA_BLOCK,
                block_num=23, last_used_time=87, valid_ratio=0.8,
                data_group_no=8)

        self.assertTrue(blkinfo1 < blkinfo2)
        self.assertTrue(blkinfo3 < blkinfo1)

    def test_priority_queue(self):
        priority_q = Queue.PriorityQueue()

        for i in range(10):
            blkinfo = BlockInfo(block_type=TYPE_DATA_BLOCK,
                block_num=23, last_used_time=i, valid_ratio=0.8,
                data_group_no=8)
            priority_q.put(blkinfo)

        used_times = []
        while not priority_q.empty():
            time  = priority_q.get().last_used_time
            used_times.append( time )

        self.assertListEqual(used_times, range(10))


class TestSingleLogBlockInfo(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        blkinfo = SingleLogBlockInfo(conf, 7, last_used_time=8,
                last_programmed_offset=1)

    def test_next_ppn(self):
        conf = create_config()
        blkinfo = SingleLogBlockInfo(conf, 7, last_used_time=8)

        n_pages_per_block = conf.n_pages_per_block
        gotit, ppn = blkinfo.next_ppn_to_program()
        self.assertTrue(gotit)
        self.assertEqual(ppn, 7*n_pages_per_block)

        gotit, ppn = blkinfo.next_ppn_to_program()
        self.assertTrue(gotit)
        self.assertEqual(ppn, 7*n_pages_per_block+1)

    def test_has_free_page(self):
        conf = create_config()
        blkinfo = SingleLogBlockInfo(conf, 7, last_used_time=8)

        self.assertTrue(blkinfo.has_free_page())

        n_pages_per_block = conf.n_pages_per_block
        for i in range(n_pages_per_block):
            self.assertTrue(blkinfo.has_free_page())
            ppn = blkinfo.next_ppn_to_program()

        self.assertFalse(blkinfo.has_free_page())


class TestOutOfBandAreas(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        oob = OutOfBandAreas(conf)

        self.assertEqual(len(oob.ppn_to_lpn), 0)

    def test_remap(self):
        conf = create_config()
        oob = OutOfBandAreas(conf)

        oob.remap(lpn=8, old_ppn=None, new_ppn=88)
        self.assertEqual(oob.translate_ppn_to_lpn(88), 8)
        self.assertEqual(oob.states.is_page_valid(88), True)

        oob.remap(lpn=8, old_ppn=88, new_ppn=89)
        self.assertEqual(oob.translate_ppn_to_lpn(89), 8)
        self.assertEqual(oob.translate_ppn_to_lpn(88), 8)
        self.assertEqual(oob.states.is_page_valid(88), False)
        self.assertEqual(oob.states.is_page_valid(89), True)

    def test_wipe_ppn(self):
        conf = create_config()
        oob = OutOfBandAreas(conf)

        oob.remap(lpn=8, old_ppn=None, new_ppn=88)
        self.assertEqual(oob.translate_ppn_to_lpn(88), 8)
        self.assertEqual(oob.states.is_page_valid(88), True)

        oob.wipe_ppn(ppn=88)
        self.assertEqual(oob.translate_ppn_to_lpn(88), 8)
        self.assertEqual(oob.states.is_page_valid(88), False)

    def test_erase_block(self):
        conf = create_config()
        oob = OutOfBandAreas(conf)

        n_pages_per_block = conf.n_pages_per_block

        ppns = range(1*n_pages_per_block, 1*n_pages_per_block+3)
        for lpn, ppn in zip([3, 88, 23], ppns):
            oob.remap(lpn=lpn, old_ppn=None, new_ppn=ppn)

        oob.erase_block(1)

        self.assertEqual(len(oob.ppn_to_lpn), 0)
        for ppn in ppns:
            self.assertTrue(oob.states.is_page_erased(ppn))

    def test_lpns_of_block(self):
        conf = create_config()
        oob = OutOfBandAreas(conf)

        n_pages_per_block = conf.n_pages_per_block

        lpns = [3, 88, 23]
        ppns = range(1*n_pages_per_block, 1*n_pages_per_block+3)
        for lpn, ppn in zip(lpns, ppns):
            oob.remap(lpn=lpn, old_ppn=None, new_ppn=ppn)

        lpns_with_na = lpns + ['NA'] * (n_pages_per_block - 3)
        self.assertListEqual(sorted(oob.lpns_of_block(1)), sorted(lpns_with_na))


class TestLogBlockMappingTable(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        logmaptable = LogMappingTable(conf, rec, helper)

    def test_add_log_block(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        logmaptable = LogMappingTable(conf, rec, helper)
        logmaptable.add_log_block(dgn=1, flash_block=8)

        self.assertEqual(logmaptable.log_group_info.keys()[0], 1)
        self.assertEqual(
                logmaptable.log_group_info.values()[0]._log_blocks.keys()[0], 8)

    def test_next_ppn_error(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        logmaptable = LogMappingTable(conf, rec, helper)
        gotit, err = logmaptable.next_ppn_to_program(dgn=1)
        self.assertEqual(gotit, False)
        self.assertEqual(err, ERR_NEED_NEW_BLOCK)

    def test_add_mapping(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        logmaptable = LogMappingTable(conf, rec, helper)

        logmaptable.add_log_block(dgn=1, flash_block=8)

        gotit, ppn = logmaptable.next_ppn_to_program(dgn=1)
        self.assertEqual(gotit, True)

        n_blocks_in_data_group = conf['nkftl']['n_blocks_in_data_group']
        n_pages_per_block = conf.n_pages_per_block
        n_pages_per_dg = n_blocks_in_data_group * n_pages_per_block
        lpn = n_pages_per_dg + 2
        logmaptable.add_mapping(data_group_no=1, lpn=lpn, ppn=ppn)

        # Test translation
        found, ppn_retrieved = logmaptable.lpn_to_ppn(lpn)
        self.assertEqual(found, True)
        self.assertEqual(ppn_retrieved, ppn)

        # Test removing
        logmaptable.remove_lpn(data_group_no=1, lpn=lpn)

        found, ppn_retrieved = logmaptable.lpn_to_ppn(lpn)
        self.assertEqual(found, False)


class TestDataBlockMappingTable(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        datablocktable = DataBlockMappingTable(conf, rec, helper)

    def test_adding(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        datablocktable = DataBlockMappingTable(conf, rec, helper)

        n_pages_per_block = conf.n_pages_per_block
        datablocktable.add_data_block_mapping(1, 8)

        found, pbn = datablocktable.lbn_to_pbn(1)
        self.assertEqual(found, True)
        self.assertEqual(pbn, 8)

        found, ppn = datablocktable.lpn_to_ppn(0)
        self.assertEqual(found, False)

        found, ppn = datablocktable.lpn_to_ppn(n_pages_per_block + 1)
        self.assertEqual(found, True)
        self.assertEqual(ppn, 8*n_pages_per_block + 1)

    def test_removing(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)

        datablocktable = DataBlockMappingTable(conf, rec, helper)

        n_pages_per_block = conf.n_pages_per_block
        datablocktable.add_data_block_mapping(1, 8)

        found, pbn = datablocktable.lbn_to_pbn(1)
        self.assertEqual(found, True)
        self.assertEqual(pbn, 8)

        datablocktable.remove_data_block_mapping(1)
        found, _ = datablocktable.lbn_to_pbn(1)
        self.assertEqual(found, False)


class TestGcDecider(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)

        gcdecider = GcDecider(conf, block_pool, rec)

    def test_high_threshold(self):
        conf = create_config()
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)

        n = len(block_pool.freeblocks)
        high_blocks = conf['nkftl']['GC_threshold_ratio'] * n
        low_blocks = conf['nkftl']['GC_low_threshold_ratio'] * n
        diff = high_blocks - low_blocks
        print 'high..', high_blocks

        gcdecider = GcDecider(conf, block_pool, rec)

        blocks = []
        for i in range(int(high_blocks)):
            blk = block_pool.pop_a_free_block_to_log_blocks()
            blocks.append(blk)
            gcdecider.refresh()
            self.assertEqual(gcdecider.need_cleaning(), False)

        block_pool.pop_a_free_block_to_log_blocks()
        gcdecider.refresh()
        self.assertEqual(gcdecider.need_cleaning(), True)

        for i in range(int(diff)):
            block_pool.free_used_log_block(blocks[i])
            self.assertEqual(gcdecider.need_cleaning(), True)

        block_pool.free_used_log_block(blocks[i+1])
        block_pool.free_used_log_block(blocks[i+2])
        self.assertEqual(gcdecider.need_cleaning(), False)


class TestVictimBlocks(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        vblocks = VictimDataBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

    def test_empty_victims_log(self):
        conf = create_config()
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        cnt = 0
        for blkinfo in vblocks:
            cnt += 1

        self.assertEqual(cnt, 0)

    def test_empty_victims_data(self):
        conf = create_config()
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        vblocks = VictimDataBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        cnt = 0
        for blkinfo in vblocks:
            cnt += 1

        self.assertEqual(cnt, 0)

    @unittest.skip("")
    def test_one_victim_blocks(self):
        conf = create_config()
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        # use one block
        self.use_a_log_block(conf, oob, block_pool)

        # check the block
        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)
        self.assertEqual(len(vblocks), 1)

    def use_a_log_block(self, conf, oob, block_pool, logmapping):
        states = oob.states

        cnt = 2 * conf.n_pages_per_block + 1
        while cnt > 0:
            found, ppn = logmapping.next_ppn_to_program(dgn=1)
            if found is False and ppn == ERR_NEED_NEW_BLOCK:
                blocknum = block_pool.pop_a_free_block_to_log_blocks()
                logmapping.add_log_block(dgn=1, flash_block=blocknum)
            else:
                # got a page
                # invalidate it
                states.invalidate_page(ppn)
                cnt -= 1

    def test_log_used(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        self.use_a_log_block(conf, oob, block_pool, logmaptable)

        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        self.assertEqual(len(vblocks), 2)

    def test_data_used(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        self.use_a_data_block(conf, block_pool, oob)
        self.use_a_data_block(conf, block_pool, oob)

        vblocks = VictimDataBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        self.assertEqual(len(vblocks), 2)

    def use_a_data_block(self, conf, block_pool, oob):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        start, end = conf.block_to_page_range(blocknum)

        for ppn in range(start, end):
            oob.states.invalidate_page(ppn)


class TestCleaningDataBlocks(unittest.TestCase):
    def test_init_gc(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flash, oob, rec,
                translator, helper, logmaptable, datablocktable)

    def test_clean_data_blocks(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = BlockPool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        lpb, blocknum = self.use_a_data_block(conf, block_pool, oob, datablocktable)

        gc.recycle_empty_data_block(blocknum, tag="")

        # states bitmap should be in 'erased' state



        # oob ppn->lpn mapping should hold nothing
        # blocknum should be free block in block_pool
        # datablocktable should not hold mapping of blocknum
        # not more victim blocks


    def use_a_data_block(self, conf, block_pool, oob, datablocktable):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        # mapping still exist
        datablocktable.add_data_block_mapping(lbn=8, pbn=blocknum)

        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            oob.states.invalidate_page(ppn)

        return lpb, blocknum

    def use_a_data_block_no_mapping(self, conf, block_pool, oob, datablocktable):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            oob.states.invalidate_page(ppn)

        return blocknum








def main():
    unittest.main()

if __name__ == '__main__':
    main()


