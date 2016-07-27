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
from ssdbox.ftlsim_commons import *

TDATA = 'TDATA'
TLOG = 'TLOG'

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

    conf['n_pages_per_region'] = conf.n_pages_per_block

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


def create_nkblockpool(conf):
    block_pool = NKBlockPool(
            n_channels=conf.n_channels_per_dev,
            n_blocks_per_channel=conf.n_blocks_per_channel,
            n_pages_per_block=conf.n_pages_per_block,
            tags=[TDATA, TLOG])
    return block_pool


def create_nkftl():
    conf = create_config()
    rec = create_recorder(conf)

    ftl = Ftl(conf, rec,
        ssdbox.flash.Flash(recorder=rec, confobj=conf))
    return ftl, conf, rec

def create_global_helper(conf):
    return GlobalHelper(conf)

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


class TestBlockPool(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_log_blocks(self):
        conf = create_config()
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

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
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        self.assertIn(blocknum, block_pool.data_usedblocks)
        self.assertNotIn(blocknum, block_pool.log_usedblocks)

        block_pool.free_used_data_block(blocknum)
        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_free_used_log(self):
        conf = create_config()
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

        blocknum = block_pool.pop_a_free_block_to_log_blocks()
        block_pool.free_used_log_block(blocknum)
        self.assertEqual(block_pool.used_ratio(), 0)
        self.assertEqual(block_pool.total_used_blocks(), 0)

    def test_freeblocks(self):
        conf = create_config()
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

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
        block_pool = create_nkblockpool(conf)

        logmaptable = LogMappingTable(conf, block_pool, rec, helper)

    def test_add_mapping(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

        logmaptable = LogMappingTable(conf, block_pool, rec, helper)

        lpns = []
        n_pages_per_data_group = conf.n_pages_per_data_group()
        for lpn in range(n_pages_per_data_group, n_pages_per_data_group + 4):
            lpns.append(lpn)

        ppns = logmaptable.next_ppns_to_program(dgn=1, n=4, strip_unit_size=4)

        for lpn, ppn in zip(lpns, ppns):
            logmaptable.add_mapping(lpn=lpn, ppn=ppn)

        # Test translation
        for lpn, ppn in zip(lpns, ppns):
            found, ppn_retrieved = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            self.assertEqual(ppn_retrieved, ppn)

        # Test removing
        logmaptable.remove_lpn(lpn=lpn)

        found, ppn_retrieved = logmaptable.lpn_to_ppn(lpn)
        self.assertEqual(found, False)

    def test_next_ppns(self):
        conf = create_config()
        rec = create_recorder(conf)
        helper = create_global_helper(conf)
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

        logmaptable = LogMappingTable(conf, block_pool, rec, helper)

        ppns = logmaptable.next_ppns_to_program(dgn=1, n=4, strip_unit_size=4)
        self.assertEqual(len(ppns), 4)




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
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)

        gcdecider = GcDecider(conf, block_pool, rec)

    def test_high_threshold(self):
        conf = create_config()
        block_pool = create_nkblockpool(conf)
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
            self.assertEqual(gcdecider.should_start(), False)

        block_pool.pop_a_free_block_to_log_blocks()
        self.assertEqual(gcdecider.should_start(), True)

        for i in range(int(diff)):
            block_pool.free_used_log_block(blocks[i])
            self.assertEqual(gcdecider.should_stop(), False)

        block_pool.free_used_log_block(blocks[i+1])
        block_pool.free_used_log_block(blocks[i+2])
        self.assertEqual(gcdecider.should_stop(), True)


class TestVictimBlocks(unittest.TestCase):
    def test_init(self):
        conf = create_config()
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        vblocks = VictimDataBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

    def test_empty_victims_log(self):
        conf = create_config()
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        cnt = 0
        for blkinfo in vblocks:
            cnt += 1

        self.assertEqual(cnt, 0)

    def test_empty_victims_data(self):
        conf = create_config()
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        vblocks = VictimDataBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        cnt = 0
        for blkinfo in vblocks:
            cnt += 1

        self.assertEqual(cnt, 0)

    def test_one_victim_blocks(self):
        conf = create_config()
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        # use one block
        # +1 is because current log block may not be a victim
        self.use_a_log_block(conf, oob, block_pool, logmaptable,
                cnt=conf.n_pages_per_block+1, dgn=1)

        # check the block
        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)
        self.assertEqual(len(vblocks), 2)

    def use_a_log_block(self, conf, oob, block_pool, logmapping, cnt, dgn):
        states = oob.states

        remaining = cnt
        while remaining > 0:
            ppns = logmapping.next_ppns_to_program(dgn=dgn,
                n=remaining, strip_unit_size='infinity')
            remaining = remaining - len(ppns)

            # invalidate them (not the same as in production)
            for ppn in ppns:
                states.invalidate_page(ppn)

    def test_log_used(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)

        self.use_a_log_block(conf, oob, block_pool, logmaptable,
                cnt=2*conf.n_pages_per_block+1, dgn=1)

        vblocks = VictimLogBlocks(conf, block_pool, oob, rec, logmaptable,
                datablocktable)

        self.assertEqual(len(vblocks), 3)

    def test_data_used(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
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
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

    def test_clean_data_blocks(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        lbn = 8
        blocknum = self.use_a_data_block(conf, block_pool, oob, datablocktable, lbn)

        gc.recycle_empty_data_block(blocknum, tag="")

        # states bitmap should be in 'erased' state
        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # oob ppn->lpn mapping should hold nothing
        for ppn in range(start, end):
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # blocknum should be free block in block_pool
        self.assertIn(blocknum, block_pool.freeblocks)
        self.assertNotIn(blocknum, block_pool.data_usedblocks)

        # datablocktable should not hold mapping of blocknum
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)

        # not more victim blocks
        self.assertEqual(len(block_pool.data_usedblocks), 0)

    @unittest.skip("data block without mapping is impossible. (really?)")
    def test_clean_data_blocks_without_mapping(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        lbn = 8
        blocknum = self.use_a_data_block_no_mapping(conf, block_pool,
                oob, datablocktable)

        gc.recycle_empty_data_block(blocknum, tag="")

        # states bitmap should be in 'erased' state
        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # oob ppn->lpn mapping should hold nothing
        for ppn in range(start, end):
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # blocknum should be free block in block_pool
        self.assertIn(blocknum, block_pool.freeblocks)
        self.assertNotIn(blocknum, block_pool.data_usedblocks)

        # datablocktable should not hold mapping of blocknum
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)

        # not more victim blocks
        self.assertEqual(len(block_pool.data_usedblocks), 0)

    def use_a_data_block(self, conf, block_pool, oob, datablocktable, lbn):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        # mapping still exist
        datablocktable.add_data_block_mapping(lbn=lbn, pbn=blocknum)

        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            oob.states.invalidate_page(ppn)

        return blocknum

    def use_a_data_block_no_mapping(self, conf, block_pool, oob, datablocktable):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            oob.states.invalidate_page(ppn)

        return blocknum


class UseLogBlocksMixin(object):
    def use_log_blocks(self, conf, oob, block_pool,
            logmapping, cnt, lpn_start, translator):
        dgn = conf.nkftl_data_group_number_of_lpn(lpn_start)
        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmapping, cnt=cnt, dgn=dgn)

        self.assertEqual(len(ppns), cnt)
        lpns = range(lpn_start, lpn_start + cnt)
        self.set_mappings(oob, block_pool, logmapping, lpns, ppns,
                translator)

        return used_blocks

    def get_ppns_from_data_group(self,
            conf, oob, block_pool, logmapping, cnt, dgn):
        used_blocks = []

        ppns = logmapping.next_ppns_to_program(dgn=dgn, n=cnt,
                strip_unit_size='infinity')
        for ppn in ppns:
            block, _ = conf.page_to_block_off(ppn)
            if block not in used_blocks:
                used_blocks.append(block)

        return used_blocks, ppns

    def page_ext(self, start, cnt):
        return range(start, start + cnt)

    def set_mappings(self, oob, block_pool, logmapping, lpns, ppns,
            translator):
        states = oob.states
        for lpn, ppn in zip(lpns, ppns):
            # oob states
            states.validate_page(ppn)
            # oob ppn->lpn
            found, old_ppn, _ = translator.lpn_to_ppn(lpn)
            if found is True and not oob.states.is_page_valid(old_ppn):
                old_ppn = None
            oob.remap(lpn=lpn, old_ppn=old_ppn, new_ppn=ppn)
            # data block mapping
            pass
            # log block mapping
            logmapping.add_mapping(lpn=lpn, ppn=ppn)


class TestSwitchMerge(unittest.TestCase, UseLogBlocksMixin):
    def test_is_switch_mergable(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block,
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn = gc.is_switch_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, True)
        self.assertEqual(lbn, 1)

    def test_is_not_switch_mergable_half_used(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn = gc.is_switch_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

    def test_is_not_switch_mergable(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block+1,
                lpn_start=conf.n_pages_per_block+1,
                translator=translator)

        mergable, lbn = gc.is_switch_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

    def test_switch_merge(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block+1,
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        lbn = 1
        pbn = used_blocks[0]

        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn, i)
            self.assertEqual(ppn, correct_ppn)
        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        # block pool
        self.assertIn(pbn, block_pool.log_usedblocks)

        gc.switch_merge(log_pbn=pbn, logical_block=lbn)

        # data block mapping
        found, retrieved_pbn = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, True)
        self.assertEqual(retrieved_pbn, pbn)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
            self.assertEqual(ppn, None)
        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        # block pool
        self.assertIn(pbn, block_pool.data_usedblocks)


class TestPartialMerge(unittest.TestCase, UseLogBlocksMixin):
    def test_is_partial_mergable(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, True)
        self.assertEqual(lbn, 1)
        self.assertEqual(off, int(conf.n_pages_per_block/2))

    def test_is_not_partial_mergable_not_aligned(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block+1,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

    def test_is_not_partial_mergable_because_its_switch_mergable(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block,
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

    def test_partial_merge(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, True)
        self.assertEqual(lbn, 1)
        self.assertEqual(off, int(conf.n_pages_per_block/2))

        pbn = used_blocks[0]
        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)
        # log mapping
        for i in range(off):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn, i)
            self.assertEqual(ppn, correct_ppn)
        # oob states
        for i in range(off):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(off, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        # oob ppn->lpn
        for i in range(off):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(off, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        # block pool
        self.assertIn(pbn, block_pool.log_usedblocks)

        gc.partial_merge(log_pbn=used_blocks[0], lbn=lbn, first_free_offset=off)

        # data block mapping
        found, retrieved_pbn = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, True)
        self.assertEqual(retrieved_pbn, pbn)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
            self.assertEqual(ppn, None)
        # oob states
        for i in range(off):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(off, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        # oob ppn->lpn
        for i in range(off):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(off, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        # block pool
        self.assertIn(pbn, block_pool.data_usedblocks)

    def test_partial_merge_with_moving(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        half_block_pages = int(conf.n_pages_per_block/2)

        ##########################
        # unaligned, second half, first half
        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmaptable, cnt=half_block_pages * 3,
                dgn=0)
        lpns = self.page_ext(conf.n_pages_per_block + 1, half_block_pages) +\
               self.page_ext(2 * conf.n_pages_per_block + half_block_pages, half_block_pages) +\
               self.page_ext(2 * conf.n_pages_per_block, half_block_pages)
        self.set_mappings(oob, block_pool, logmaptable, lpns, ppns,
                translator)
        self.assertEqual(len(used_blocks), 2)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[1])
        self.assertEqual(mergable, True)
        self.assertEqual(lbn, 2)
        self.assertEqual(off, half_block_pages)

        mergable, lbn1, off1 = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn1, None)
        self.assertEqual(off1, None)

        pbn = used_blocks[1]
        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)
        # log mapping
        for i in range(off):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn, i)
            self.assertEqual(ppn, correct_ppn)
        # oob states
        for i in range(off):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(off, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        # oob ppn->lpn
        for i in range(off):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(off, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        # block pool
        self.assertIn(pbn, block_pool.log_usedblocks)

        gc.partial_merge(log_pbn=pbn, lbn=lbn, first_free_offset=off)

        # data block mapping
        found, retrieved_pbn = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, True)
        self.assertEqual(retrieved_pbn, pbn)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
            self.assertEqual(ppn, None)
        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        # block pool
        self.assertIn(pbn, block_pool.data_usedblocks)


class TestFullMerge(unittest.TestCase, UseLogBlocksMixin):
    def test_full_merge_unaligned(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        half_block_pages = int(conf.n_pages_per_block/2)


        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmaptable, cnt=half_block_pages * 2,
                dgn=0)
        lpns = self.page_ext(conf.n_pages_per_block + half_block_pages, half_block_pages) +\
               self.page_ext(conf.n_pages_per_block, half_block_pages)
        self.set_mappings(oob, block_pool, logmaptable, lpns, ppns,
                translator)
        self.assertEqual(len(used_blocks), 1)

        pbn = used_blocks[0]
        lbn = 1

        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn,
                    (i + half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(ppn, correct_ppn)
        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn,
                    (i + half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        # block pool
        self.assertIn(pbn, block_pool.log_usedblocks)

        gc.full_merge(log_pbn=pbn)

        # data block mapping
        found, retrieved_pbn = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, True)
        self.assertNotEqual(retrieved_pbn, pbn)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
            self.assertEqual(ppn, None)
        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # block pool
        self.assertIn(retrieved_pbn, block_pool.data_usedblocks)
        self.assertIn(pbn, block_pool.freeblocks)

    def test_full_merge_two_in_two(self):
        """
        Data of two logical blocks spread in two physical blocks.
        """
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        conf['nkftl']['n_blocks_in_data_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        half_block_pages = int(conf.n_pages_per_block/2)


        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmaptable, cnt=half_block_pages * 4,
                dgn=0)
        lbn1 = 1
        lbn2 = 3
        lpns = self.page_ext(lbn1 * conf.n_pages_per_block + half_block_pages, half_block_pages) +\
               self.page_ext(lbn2 * conf.n_pages_per_block + half_block_pages, half_block_pages) +\
               self.page_ext(lbn1 * conf.n_pages_per_block, half_block_pages) +\
               self.page_ext(lbn2 * conf.n_pages_per_block, half_block_pages)
        self.set_mappings(oob, block_pool, logmaptable, lpns, ppns,
                translator)
        self.assertEqual(len(used_blocks), 2)

        ######## start checking ########
        pbn1 = used_blocks[0]
        pbn2 = used_blocks[1]

        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, False)
        found, _ = datablocktable.lbn_to_pbn(lbn2)
        self.assertEqual(found, False)
        # log mapping
        # lbn1
        for i in range(half_block_pages):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn2, i)
            self.assertEqual(ppn, correct_ppn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn1, i - half_block_pages)
            self.assertEqual(ppn, correct_ppn)
        # lb2
        for i in range(half_block_pages):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn2, i + half_block_pages)
            self.assertEqual(ppn, correct_ppn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn1, i)
            self.assertEqual(ppn, correct_ppn)

        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    (i + half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2,
                    i % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    i % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2,
                    (i-half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)

        # block pool
        self.assertIn(pbn1, block_pool.log_usedblocks)
        self.assertIn(pbn2, block_pool.log_usedblocks)

        ########### full merge 1 ##############
        gc.full_merge(log_pbn=pbn1)

        ########### check #####################
        # data block mapping
        found, retrieved_pbn1 = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, True)
        found, retrieved_pbn2 = datablocktable.lbn_to_pbn(lbn2)
        self.assertEqual(found, True)

        # log mapping
        # should not exist
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

        # oob states
        # should have been erased
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn2, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2, i)
            self.assertEqual(lpn, correct_lpn)


        # block pool
        self.assertIn(pbn1, block_pool.freeblocks)
        self.assertIn(pbn2, block_pool.freeblocks)
        self.assertIn(retrieved_pbn1, block_pool.data_usedblocks)
        self.assertIn(retrieved_pbn2, block_pool.data_usedblocks)

        ########### the following full merge call should do nothing
        # as all logical blocks are already merged
        gc.full_merge(log_pbn=pbn2)

        ########### check #####################
        # data block mapping
        found, retrieved_pbn1 = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, True)
        found, retrieved_pbn2 = datablocktable.lbn_to_pbn(lbn2)
        self.assertEqual(found, True)

        # log mapping
        # should not exist
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

        # oob states
        # should have been erased
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn2, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2, i)
            self.assertEqual(lpn, correct_lpn)


        # block pool
        self.assertIn(pbn1, block_pool.freeblocks)
        self.assertIn(pbn2, block_pool.freeblocks)
        self.assertIn(retrieved_pbn1, block_pool.data_usedblocks)
        self.assertIn(retrieved_pbn2, block_pool.data_usedblocks)

    def test_full_merge_with_data_blocks(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        conf['nkftl']['n_blocks_in_data_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        half_block_pages = int(conf.n_pages_per_block/2)




        lbn1=3
        # put first half of lba1 in data block
        usedblocks1 = self.use_data_blocks(conf, block_pool, oob, datablocktable,
                lpn_start=lbn1 * conf.n_pages_per_block,
                cnt=half_block_pages, translator=translator)
        self.assertEqual(len(usedblocks1), 1)


        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmaptable, cnt=half_block_pages * 2,
                dgn=0)
        lpns = self.page_ext(lbn1 * conf.n_pages_per_block + half_block_pages, half_block_pages)
        self.set_mappings(oob, block_pool, logmaptable, lpns, ppns,
                translator)
        self.assertEqual(len(used_blocks), 1)

        data_pbn = usedblocks1[0]
        log_pbn = used_blocks[0]

        ################# check
        # data block mapping
        found, retrieved_pbn1 = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, True)
        self.assertEqual(retrieved_pbn1, data_pbn)
        # log mapping
        # lbn1
        for i in range(half_block_pages):
            lpn = conf.block_off_to_page(lbn1, half_block_pages + i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(log_pbn, i)
            self.assertEqual(ppn, correct_ppn)

        # oob states
        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(data_pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(data_pbn, i)
            print ppn
            print 'page state ..........', oob.states.page_state_human(ppn)
            self.assertTrue(oob.states.is_page_erased(ppn))

        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(log_pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(log_pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))

        # oob ppn->lpn
        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(data_pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    i % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(data_pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(log_pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    (i+half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(log_pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # block pool
        self.assertIn(data_pbn, block_pool.data_usedblocks)
        self.assertIn(log_pbn, block_pool.log_usedblocks)

        #################### Full merge ##################
        gc.full_merge(log_pbn=log_pbn)

        ################# check
        # data block mapping
        found, retrieved_pbn1 = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, True)
        # log mapping
        # lbn1
        for i in range(half_block_pages):
            lpn = conf.block_off_to_page(lbn1, half_block_pages + i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(data_pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))

            ppn = conf.block_off_to_page(log_pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))

            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(data_pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

            ppn = conf.block_off_to_page(log_pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    i % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)

        # block pool
        self.assertIn(data_pbn, block_pool.freeblocks)
        self.assertIn(log_pbn, block_pool.freeblocks)
        self.assertIn(retrieved_pbn1, block_pool.data_usedblocks)

    def test_full_merge_one_page_used(self):
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        half_block_pages = int(conf.n_pages_per_block/2)

        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmaptable, cnt=1,
                dgn=0)
        lbn = 1
        lpns = self.page_ext(lbn * conf.n_pages_per_block + half_block_pages, 1)
        self.set_mappings(oob, block_pool, logmaptable, lpns, ppns,
                translator)
        self.assertEqual(len(used_blocks), 1)

        pbn = used_blocks[0]

        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, False)
        # log mapping
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            if i == half_block_pages:
                self.assertEqual(found, True)
                correct_ppn = conf.block_off_to_page(pbn, 0)
                self.assertEqual(ppn, correct_ppn)
            else:
                self.assertEqual(found, False)
        # oob states
        for i in range(1):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        # oob ppn->lpn
        for i in range(1):
            ppn = conf.block_off_to_page(pbn, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn, half_block_pages)
            self.assertEqual(lpn, correct_lpn)
        # block pool
        self.assertIn(pbn, block_pool.log_usedblocks)

        gc.full_merge(log_pbn=pbn)

        # data block mapping
        found, retrieved_pbn = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, True)
        self.assertNotEqual(retrieved_pbn, pbn)
        # log mapping
        for i in range(1):
            lpn = conf.block_off_to_page(lbn, half_block_pages)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
            self.assertEqual(ppn, None)
        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(1):
            ppn = conf.block_off_to_page(retrieved_pbn, half_block_pages)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn, i)
            if i != half_block_pages:
                with self.assertRaises(KeyError):
                    oob.translate_ppn_to_lpn(ppn)
            else:
                lpn = oob.translate_ppn_to_lpn(ppn)
                correct_lpn = conf.block_off_to_page(lbn, i)
                self.assertEqual(lpn, correct_lpn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # block pool
        self.assertIn(retrieved_pbn, block_pool.data_usedblocks)
        self.assertIn(pbn, block_pool.freeblocks)

    def use_data_blocks(self, conf, block_pool, oob, datablocktable, lpn_start,
            cnt, translator):

        lpn = lpn_start
        used_blocks = []
        while cnt > 0:
            lbn, off = conf.page_to_block_off(lpn)
            found, _ = datablocktable.lbn_to_pbn(lbn)
            if found is False:
                blocknum = block_pool.pop_a_free_block_to_data_blocks()
                used_blocks.append(blocknum)
                datablocktable.add_data_block_mapping(lbn=lbn, pbn=blocknum)
            else:
                found, ppn = datablocktable.lpn_to_ppn(lpn)
                self.assertEqual(found, True)

                found, old_ppn, _ = translator.lpn_to_ppn(lpn)
                if found is True and not oob.states.is_page_valid(old_ppn):
                    old_ppn = None

                oob.remap(lpn=lpn, old_ppn=old_ppn, new_ppn=ppn)

                lpn += 1
                cnt -= 1
        return used_blocks


class TestCleanDataGroup(unittest.TestCase, UseLogBlocksMixin):
    def test_full_merge_two_in_two(self):
        """
        Data of two logical blocks spread in two physical blocks.
        """
        conf = create_config()
        conf['nkftl']['max_blocks_in_log_group'] = 4
        conf['nkftl']['n_blocks_in_data_group'] = 4
        block_pool = create_nkblockpool(conf)
        rec = create_recorder(conf)
        oob = OutOfBandAreas(conf)
        helper = create_global_helper(conf)
        logmaptable = LogMappingTable(conf, block_pool, rec, helper)
        datablocktable = DataBlockMappingTable(conf, rec, helper)
        translator = Translator(conf, rec, helper, logmaptable, datablocktable)
        flashobj = flash.SimpleFlash(recorder=rec, confobj=conf)

        gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
                translator, helper, logmaptable, datablocktable)

        half_block_pages = int(conf.n_pages_per_block/2)

        used_blocks, ppns = self.get_ppns_from_data_group(
                conf, oob, block_pool, logmaptable, cnt=half_block_pages * 4,
                dgn=0)
        lbn1 = 1
        lbn2 = 3
        lpns = self.page_ext(lbn1 * conf.n_pages_per_block + half_block_pages, half_block_pages) +\
               self.page_ext(lbn2 * conf.n_pages_per_block + half_block_pages, half_block_pages) +\
               self.page_ext(lbn1 * conf.n_pages_per_block, half_block_pages) +\
               self.page_ext(lbn2 * conf.n_pages_per_block, half_block_pages)
        self.set_mappings(oob, block_pool, logmaptable, lpns, ppns,
                translator)
        self.assertEqual(len(used_blocks), 2)



        ######## start checking ########
        pbn1 = used_blocks[0]
        pbn2 = used_blocks[1]

        # data block mapping
        found, _ = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, False)
        found, _ = datablocktable.lbn_to_pbn(lbn2)
        self.assertEqual(found, False)
        # log mapping
        # lbn1
        for i in range(half_block_pages):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn2, i)
            self.assertEqual(ppn, correct_ppn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn1, i - half_block_pages)
            self.assertEqual(ppn, correct_ppn)
        # lb2
        for i in range(half_block_pages):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn2, i + half_block_pages)
            self.assertEqual(ppn, correct_ppn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            correct_ppn = conf.block_off_to_page(pbn1, i)
            self.assertEqual(ppn, correct_ppn)

        # oob states
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    (i + half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2,
                    i % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages):
            ppn = conf.block_off_to_page(pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1,
                    i % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)
        for i in range(half_block_pages, conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2,
                    (i-half_block_pages) % conf.n_pages_per_block)
            self.assertEqual(lpn, correct_lpn)

        # block pool
        self.assertIn(pbn1, block_pool.log_usedblocks)
        self.assertIn(pbn2, block_pool.log_usedblocks)

        ########### clean ##############
        gc.clean_data_group(
            data_group_no=conf.nkftl_data_group_number_of_logical_block(lbn1))

        ########### check #####################
        # data block mapping
        found, retrieved_pbn1 = datablocktable.lbn_to_pbn(lbn1)
        self.assertEqual(found, True)
        found, retrieved_pbn2 = datablocktable.lbn_to_pbn(lbn2)
        self.assertEqual(found, True)

        # log mapping
        # should not exist
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn1, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)
        for i in range(conf.n_pages_per_block):
            lpn = conf.block_off_to_page(lbn2, i)
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

        # oob states
        # should have been erased
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            self.assertTrue(oob.states.is_page_erased(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            self.assertTrue(oob.states.is_page_valid(ppn))
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn2, i)
            self.assertTrue(oob.states.is_page_valid(ppn))

        # oob ppn->lpn
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn1, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(pbn2, i)
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn1, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn1, i)
            self.assertEqual(lpn, correct_lpn)
        for i in range(conf.n_pages_per_block):
            ppn = conf.block_off_to_page(retrieved_pbn2, i)
            lpn = oob.translate_ppn_to_lpn(ppn)
            correct_lpn = conf.block_off_to_page(lbn2, i)
            self.assertEqual(lpn, correct_lpn)

        # block pool
        self.assertIn(pbn1, block_pool.freeblocks)
        self.assertIn(pbn2, block_pool.freeblocks)
        self.assertIn(retrieved_pbn1, block_pool.data_usedblocks)
        self.assertIn(retrieved_pbn2, block_pool.data_usedblocks)

    def use_data_blocks(self, conf, block_pool, oob, datablocktable, lpn_start,
            cnt, translator):

        lpn = lpn_start
        used_blocks = []
        while cnt > 0:
            lbn, off = conf.page_to_block_off(lpn)
            found, _ = datablocktable.lbn_to_pbn(lbn)
            if found is False:
                blocknum = block_pool.pop_a_free_block_to_data_blocks()
                used_blocks.append(blocknum)
                datablocktable.add_data_block_mapping(lbn=lbn, pbn=blocknum)
            else:
                found, ppn = datablocktable.lpn_to_ppn(lpn)
                self.assertEqual(found, True)

                found, old_ppn, _ = translator.lpn_to_ppn(lpn)
                if found is True and not oob.states.is_page_valid(old_ppn):
                    old_ppn = None

                oob.remap(lpn=lpn, old_ppn=old_ppn, new_ppn=ppn)

                lpn += 1
                cnt -= 1
        return used_blocks


def create_config_2():
    conf = ssdbox.nkftl2.Config()

    conf['flash_config']['n_pages_per_block'] = 32
    conf['flash_config']['n_blocks_per_plane'] = 64
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 8

    conf['nkftl']['max_blocks_in_log_group'] = 16
    conf['nkftl']['n_blocks_in_data_group'] = 4

    conf['n_pages_per_region'] = conf.n_pages_per_block

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    logicsize_mb = 64
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


def create_loggroup2():
    conf = create_config_2()
    blockpool = NKBlockPool(
            n_channels=conf.n_channels_per_dev,
            n_blocks_per_channel=conf.n_blocks_per_channel,
            n_pages_per_block=conf.n_pages_per_block,
            tags=[TDATA, TLOG])
    loggroup = LogGroup2(
            conf = conf,
            block_pool=blockpool,
            max_n_log_blocks=conf['nkftl']['max_blocks_in_log_group'])

    return blockpool, loggroup


class TestLogGroup2(unittest.TestCase):
    @unittest.skip("")
    def test_next_ppns_in_channel(self):
        blockpool, loggroup = create_loggroup2()

        found, ppns = loggroup._next_ppns_in_channel(n=1)

        self.assertEqual(found, True)
        self.assertEqual(len(ppns), 1)

    @unittest.skip("")
    def test_next_ppns_in_channel_failure(self):
        blockpool, loggroup = create_loggroup2()
        n_pages_per_block = blockpool.n_pages_per_block

        found, ppns = loggroup._next_ppns_in_channel(n=n_pages_per_block + 1)

        self.assertEqual(found, False)
        self.assertEqual(len(ppns), 0)

    def test_incr_channel(self):
        conf = create_config_2()
        blockpool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])
        loggroup = LogGroup2(
                conf = conf,
                block_pool=blockpool,
                max_n_log_blocks=conf['nkftl']['max_blocks_in_log_group'])

        self.assertEqual(loggroup._cur_channel, 0)
        ret = loggroup._get_and_incr_cur_channel()
        self.assertEqual(ret, 0)
        self.assertEqual(loggroup._cur_channel, 1)

        for i in range(8):
            ret = loggroup._get_and_incr_cur_channel()

        self.assertEqual(loggroup._cur_channel, 1)

    def test_allocate_blocks_in_channel(self):
        blockpool, loggroup = create_loggroup2()
        n_channels = blockpool.n_channels

        self.assertEqual(loggroup.n_log_blocks(), 0)

        allocated = loggroup._allocate_block_in_channel(channel_id=2)

        self.assertEqual(allocated, True)
        self.assertEqual(loggroup.n_log_blocks(), 1)
        self.assertEqual(loggroup.n_channel_free_pages(channel_id=2), 32)

        allocated = loggroup._allocate_block_in_channel(channel_id=2)

        self.assertEqual(allocated, True)
        self.assertEqual(loggroup.n_log_blocks(), 2)
        self.assertEqual(loggroup.n_channel_free_pages(channel_id=2), 64)

        for i in range(16-2):
            loggroup._allocate_block_in_channel(channel_id=0)

        self.assertEqual(loggroup.n_log_blocks(), 16)
        self.assertEqual(loggroup.n_channel_free_pages(channel_id=0),
                32 * (16 - 2))

        allocated = loggroup._allocate_block_in_channel(channel_id=0)
        self.assertEqual(allocated, False)
        self.assertEqual(loggroup.n_log_blocks(), 16)

    def test_next_ppns_simple(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup.next_ppns(n=8, strip_unit_size=1)

        self.assertEqual(len(ppns), 8)
        self.assertEqual(loggroup.n_log_blocks(), 8)
        for i in range(8):
            self.assertEqual(loggroup.n_channel_free_pages(i), 32 - 1)

    def test_next_ppns_two_blocks_per_channel(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup.next_ppns(n=33*8, strip_unit_size=32)

        self.assertEqual(len(ppns), 33*8)
        self.assertEqual(loggroup.n_log_blocks(), 9)
        self.assertEqual(loggroup.n_channel_free_pages(0), 32-8)
        for i in range(1, 8):
            self.assertEqual(loggroup.n_channel_free_pages(i), 0)

    def test_next_ppns_overflow_it(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup.next_ppns(n=32*2*8 + 1, strip_unit_size=32)

        self.assertEqual(len(ppns), 32*2*8)
        self.assertEqual(loggroup.n_log_blocks(), 8*2)
        for i in range(8):
            self.assertEqual(loggroup.n_channel_free_pages(i), 0)

    def test_next_ppns_infinit_stip(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup.next_ppns(n=33, strip_unit_size='infinity')

        self.assertEqual(len(ppns), 33)
        self.assertEqual(loggroup.n_log_blocks(), 2)

    def test_next_ppns_small_strip(self):
        conf = create_config_2()
        conf['nkftl']['max_blocks_in_log_group'] = 2
        blockpool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])
        loggroup = LogGroup2(
                conf = conf,
                block_pool=blockpool,
                max_n_log_blocks=conf['nkftl']['max_blocks_in_log_group'])

        ppns = loggroup.next_ppns(n=32, strip_unit_size=16)
        self.assertEqual(len(ppns), 32)

        ppns = loggroup.next_ppns(n=32, strip_unit_size=16)
        self.assertEqual(len(ppns), 32)

        ppns = loggroup.next_ppns(n=32, strip_unit_size=16)
        self.assertEqual(len(ppns), 0)
        self.assertEqual(loggroup.n_log_blocks(), 2)

    def test_next_ppns_overflow_channel(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup.next_ppns(n=65, strip_unit_size='infinity')

        self.assertEqual(len(set(ppns)), 65)
        self.assertEqual(loggroup.n_log_blocks(), 3)

    def test_next_ppns_channel_position(self):
        blockpool, loggroup = create_loggroup2()

        self.assertEqual(loggroup._cur_channel, 0)
        ppns = loggroup.next_ppns(n=1, strip_unit_size=32)
        self.assertEqual(loggroup._cur_channel, 1)
        ppns = loggroup.next_ppns(n=1, strip_unit_size=32)
        self.assertEqual(loggroup._cur_channel, 2)

        ppns = loggroup.next_ppns(n=64, strip_unit_size=32)
        self.assertEqual(len(ppns), 64)
        self.assertEqual(loggroup._cur_channel, 4)

    def test_next_ppns_in_channel_with_allocation(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup._next_ppns_in_channel_with_allocation(33, channel_id=3)
        self.assertEqual(len(ppns), 33)
        self.assertEqual(loggroup.n_log_blocks(), 2)
        self.assertEqual(loggroup.n_channel_free_pages(channel_id=3), 31)

        ppns = loggroup._next_ppns_in_channel_with_allocation(33, channel_id=3)
        self.assertEqual(len(ppns), 33)
        self.assertEqual(loggroup.n_log_blocks(), 3)
        self.assertEqual(loggroup.n_channel_free_pages(channel_id=3), 30)

    def test_add_mapping(self):
        blockpool, loggroup = create_loggroup2()

        lpns = []
        ppns = []
        for i in range(16):
            tmp_ppns = loggroup.next_ppns(n=1, strip_unit_size=1)
            ppn = tmp_ppns[0]
            loggroup.add_mapping(lpn=i, ppn=ppn)
            lpns.append(i)
            ppns.append(ppn)

        for lpn, ppn in zip(lpns, ppns):
            self.assertEqual(loggroup.lpn_to_ppn(lpn)[1], ppn)

    def test_remove_log_block(self):
        blockpool, loggroup = create_loggroup2()
        conf = loggroup.conf

        tmp_ppns = loggroup.next_ppns(n=1, strip_unit_size=1)
        ppn = tmp_ppns[0]
        block, _ = conf.page_to_block_off(ppn)
        loggroup.add_mapping(lpn=1, ppn=ppn)

        loggroup.remove_log_block(block)

        self.assertEqual(len(loggroup._page_map), 0)
        self.assertEqual(loggroup.n_log_blocks(), 0)


class TestFTLOperations(unittest.TestCase):
    def test_write(self):
        ftl, conf, rec = create_nkftl()

        ext = Extent(lpn_start=1, lpn_count=20)
        ftl.write_ext(ext)

        for lpn in ext.lpn_iter():
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, True)

    def test_read(self):
        ftl, conf, rec = create_nkftl()

        ext = Extent(lpn_start=1, lpn_count=20)
        ftl.write_ext(ext)

        data = ftl.read_ext(ext)
        self.assertEqual(len(data), ext.lpn_count)

    def test_discard(self):
        ftl, conf, rec = create_nkftl()

        ext = Extent(lpn_start=1, lpn_count=20)
        ftl.write_ext(ext)

        ftl.discard_ext(ext)

        for lpn in ext.lpn_iter():
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

    def test_write_region(self):
        ftl, conf, rec = create_nkftl()

        ext = Extent(lpn_start=1, lpn_count=3)
        ftl.write_region(ext)

        ppns = []
        for lpn in ext.lpn_iter():
            found, ppn = ftl.log_mapping_table.lpn_to_ppn(lpn)
            self.assertEqual(ftl.oob.states.is_page_valid(ppn), True)
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            ppns.append(ppn)

        ftl.write_region(ext)

        ppns2 = []
        for lpn in ext.lpn_iter():
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            ppns2.append(ppn)

        for ppn1, ppn2 in zip(ppns, ppns2):
            self.assertNotEqual(ppn1, ppn2)

    def test_write_region_overflow(self):
        ftl, conf, rec = create_nkftl()

        for i in range(3):
            ext = Extent(lpn_start=0, lpn_count=8)
            ftl.write_region(ext)

            ppns = []
            for lpn in ext.lpn_iter():
                found, ppn = ftl.log_mapping_table.lpn_to_ppn(lpn)
                self.assertEqual(ftl.oob.states.is_page_valid(ppn), True)
                found, ppn, _ = ftl.lpn_to_ppn(lpn)
                self.assertEqual(found, True)
                ppns.append(ppn)

        # 1 data block with no valid pages
        # 1 data block with valid pages
        # 1 log block with valid pages
        self.assertEqual(ftl.block_pool.total_used_blocks(), 3)

    def test_write_large_extent(self):
        ftl, conf, rec = create_nkftl()

        for i in range(8):
            ext = Extent(lpn_start=1, lpn_count=conf.n_pages_per_block*63)
            print str(ext)
            ftl.write_ext(ext)

            ppns = []
            for lpn in ext.lpn_iter():
                found, ppn, loc = ftl.lpn_to_ppn(lpn)
                self.assertEqual(ftl.oob.states.is_page_valid(ppn), True)
                self.assertEqual(found, True)

    # def test_update_log_mapping(self):
        # ftl, conf, rec = create_nkftl()

        # mappings = {2:3, 4:5}
        # ftl._update_log_mappings(mappings)
        # for lpn, ppn in mappings.items():
            # found, retrieved_ppn, loc = ftl.lpn_to_ppn(lpn)
            # self.assertTrue(found)
            # self.assertEqual(retrieved_ppn, ppn)


def main():
    unittest.main()

if __name__ == '__main__':
    main()


