import unittest
import random
import simpy
from collections import namedtuple

from wiscsim.nkftl2 import *
from wiscsim import flash
import wiscsim
import config
from commons import *
from utilities import utils
from utilities.utils import choose_exp_metadata, runtime_update
from workflow import run_workflow
from config import LBAGENERATOR
from wiscsim.ftlsim_commons import *

TDATA = 'TDATA'
TLOG = 'TLOG'

def create_config():
    conf = wiscsim.nkftl2.Config()

    conf['flash_config']['n_pages_per_block'] = 8
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    conf['nkftl']['max_blocks_in_log_group'] = 2
    conf['nkftl']['n_blocks_in_data_group'] = 4

    conf['nkftl']['GC_threshold_ratio'] = 0.8
    conf['nkftl']['GC_low_threshold_ratio'] = 0.3

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    logicsize_mb = 64
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf

def create_config_1_channel():
    conf = wiscsim.nkftl2.Config()

    conf['flash_config']['n_pages_per_block'] = 32
    conf['flash_config']['n_blocks_per_plane'] = 64
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 1

    conf['nkftl']['max_blocks_in_log_group'] = 16
    conf['nkftl']['n_blocks_in_data_group'] = 4

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    utils.runtime_update(conf)

    return conf


def create_recorder(conf):
    rec = wiscsim.recorder.Recorder(output_target = conf['output_target'],
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

def create_env():
    env = simpy.Environment()
    return env

def create_flash_controller(env, conf, rec):
    flash_controller = wiscsim.controller.Controller3(
            env, conf, rec)
    return flash_controller

def create_nkftl():
    conf = create_config()
    rec = create_recorder(conf)
    env = create_env()
    des_flash = create_flash_controller(env, conf, rec)

    ftl = Ftl(conf, rec,
        wiscsim.flash.Flash(recorder=rec, confobj=conf), env,
        des_flash)
    return ftl, conf, rec, env

def create_global_helper(conf):
    return GlobalHelper(conf)

def create_translator(conf, rec, globalhelper, log_mapping, data_block_mapping):
    return Translator(conf, rec, globalhelper, log_mapping, data_block_mapping)

def create_gc():
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
    simpy_env = create_env()
    des_flash = create_flash_controller(simpy_env, conf, rec)
    logical_block_locks = LockPool(simpy_env)

    gc = GarbageCollector(conf, block_pool, flashobj, oob, rec,
            translator, helper, logmaptable, datablocktable, simpy_env,
            des_flash, logical_block_locks)

    GCPack = namedtuple('GCPack', 'gc, conf, block_pool, rec, oob, helper,' \
        'logmaptable, datablocktable, translator, flashobj, simpy_env, des_flash')

    return GCPack(gc=gc, conf=conf, block_pool=block_pool, rec=rec, oob=oob, helper=helper,
        logmaptable=logmaptable, datablocktable=datablocktable,
        translator=translator, flashobj=flashobj, simpy_env=simpy_env,
        des_flash=des_flash)


def random_data_of_extent(extent):
    data = []
    for lpn in extent.lpn_iter():
        d = str(lpn) + '.' + str(random.randint(0, 100))
        data.append(d)
    return data


class AssertFinishTestCase(unittest.TestCase):
    def setUp(self):
        self.finished = False
        print 'setting up'

    def set_finished(self):
        print 'setting finsihed'
        self.finished = True

    def tearDown(self):
        self.assertTrue(self.finished)
        print 'asserted finished'


class RWMixin(object):
    def write_proc(self, env, ftl, extent, data=None):
        yield env.process(ftl.write_ext(extent, data))

    def read_proc(self, env, ftl, extent):
        data = yield env.process(ftl.read_ext(extent))
        env.exit(data)

    def data_of_extent(self, extent):
        d = []
        for lpn in extent.lpn_iter():
            d.append(str(lpn))
        return d


class TestNkftl(AssertFinishTestCase, RWMixin):
    def test_init(self):
        ftl, conf, rec, env = create_nkftl()
        self.set_finished()

    def randomdata(self, lpn):
        return str(random.randint(0, 100))

    def test_write_and_read(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_write_and_read(env, ftl, conf))
        env.run()

    def proc_test_write_and_read(self, env, ftl, conf):
        yield env.process(ftl.lba_write(8, data='3'))
        ret = yield env.process( ftl.lba_read(8) )
        self.assertEqual(ret, '3')
        self.set_finished()


    def write_and_check(self, ftl, lpns, env):
        data_mirror = {}
        for lpn in lpns:
            data = self.randomdata(lpn)
            data_mirror[lpn] = data
            yield env.process(ftl.lba_write(lpn, data))

        for lpn, data in data_mirror.items():
            ret = yield env.process(ftl.lba_read(lpn))
            self.assertEqual(ret, data)
        self.set_finished()

    def test_data_integrity(self):
        ftl, conf, rec, env = create_nkftl()

        total_pages = conf.total_num_pages()
        lpns = random.sample(range(total_pages), 1000)

        env.process(self.write_and_check(ftl, lpns, env))
        env.run()

    def test_GC_simple(self):
        ftl, conf, rec, env = create_nkftl()

        lpns = [0] * 4 * conf.n_pages_per_block * conf['nkftl']['max_blocks_in_log_group']

        env.process(self.write_and_check(ftl, lpns, env))
        env.run()

    def test_GC_harder(self):
        ftl, conf, rec, env = create_nkftl()

        lpns = [0, 3, 1] * 4 * conf.n_pages_per_block * conf['nkftl']['max_blocks_in_log_group']

        env.process(self.write_and_check(ftl, lpns, env))
        env.run()

    def test_GC_harder2(self):
        ftl, conf, rec, env = create_nkftl()

        lpns = [0, 128, 3, 129, 1] * 4 * conf.n_pages_per_block * conf['nkftl']['max_blocks_in_log_group']

        env.process(self.write_and_check(ftl, lpns, env))
        env.run()

    @unittest.skipUnless(TESTALL == True, "Skip unless we want to test all")
    def test_GC_harder_super(self):
        ftl, conf, rec, env = create_nkftl()

        print 'total pages', conf.total_num_pages()
        lpns = [0, 128, 3, 129, 1] * 4 * conf.total_num_pages()

        env.process(self.write_and_check(ftl, lpns, env))
        env.run()


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

    def test_data_blocks_overflow(self):
        conf = create_config()
        block_pool = NKBlockPool(
                n_channels=conf.n_channels_per_dev,
                n_blocks_per_channel=conf.n_blocks_per_channel,
                n_pages_per_block=conf.n_pages_per_block,
                tags=[TDATA, TLOG])

        n = conf.n_blocks_per_dev
        for i in range(n):
            blocknum = block_pool.pop_a_free_block_to_data_blocks()
            self.assertTrue(isinstance(blocknum, int))

        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        self.assertEqual(blocknum, None)

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

        cnt = 0
        for binfo in vblocks:
            cnt += 1

        self.assertEqual(cnt, 2)

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


class TestWearLevelingVictimBlocks(AssertFinishTestCase):
    def test_data_block(self):
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

        datablock = self.use_a_data_block(conf, block_pool, oob, datablocktable)

        vblocks = WearLevelingVictimBlocks(conf, block_pool, oob, 2,
                logmaptable, datablocktable)
        blocknums = [blk for _, _, blk in vblocks.iterator_verbose()]
        self.assertListEqual(blocknums, [datablock])
        self.assertEqual(len(list(vblocks.iterator_verbose())), 1)

        self.set_finished()

    def test_log_block(self):
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

        self.use_a_log_block(conf, oob, block_pool, logmaptable,
                cnt=conf.n_pages_per_block, dgn=1)

        vblocks = WearLevelingVictimBlocks(conf, block_pool, oob, 2,
                logmaptable, datablocktable)
        blocknums = [blk for _, _, blk in vblocks.iterator_verbose()]
        self.assertEqual(len(list(vblocks.iterator_verbose())), 1)

        self.set_finished()

    def use_a_data_block(self, conf, block_pool, oob, datablocktable):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        start, end = conf.block_to_page_range(blocknum)

        for ppn in range(start, end):
            oob.states.invalidate_page(ppn)

        datablocktable.add_data_block_mapping(lbn=3, pbn=blocknum)

        return blocknum

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




class TestCleaningDataBlocks(AssertFinishTestCase):
    def test_init_gc(self):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = create_gc()

        self.set_finished()

    def test_clean_data_blocks(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(
            self.proc_test_clean_data_blocks(pk))
        simpy_env.run()


    def proc_test_clean_data_blocks(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        lbn = 8
        blocknum = self.use_a_data_block(conf, block_pool, oob, datablocktable, lbn)

        yield simpy_env.process(gc.recycle_empty_data_block(blocknum, tag=""))

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

        self.set_finished()

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


class TestMovingDataBlocks(AssertFinishTestCase):
    def test_init_gc(self):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = create_gc()

        self.set_finished()

    def test_move_data_blocks(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(
            self.proc_test_move_data_blocks(pk))
        simpy_env.run()

    def proc_test_move_data_blocks(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        lbn = 8
        pbn = self.use_a_data_block(conf, block_pool, oob, datablocktable, lbn)

        dst_pbn = block_pool.pop_a_free_block_to_data_blocks()
        yield simpy_env.process(gc._move_data_block(pbn, dst_pbn))

        # states bitmap should be in 'erased' state
        start, end = conf.block_to_page_range(pbn)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # oob ppn->lpn mapping should hold nothing
        for ppn in range(start, end):
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # new states bitmap should be in 'valid' state,
        # since we have all pages valid in the src pbn
        start, end = conf.block_to_page_range(dst_pbn)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_valid(ppn), True)

        for ppn in range(start, end):
            _, ppn_off = conf.page_to_block_off(ppn)
            lpn = oob.translate_ppn_to_lpn(ppn)
            _, lpn_off = conf.page_to_block_off(lpn)
            self.assertEqual(ppn_off, lpn_off)

        # pbn should be free block in block_pool
        self.assertIn(pbn, block_pool.freeblocks)
        self.assertNotIn(pbn, block_pool.data_usedblocks)

        # datablocktable should hold mapping of pbn
        found, new_pbn = datablocktable.lbn_to_pbn(lbn)
        self.assertEqual(found, True)
        self.assertNotEqual(new_pbn, pbn)

        # the new physical block should be in data used blocks
        self.assertEqual(len(block_pool.data_usedblocks), 1)

        self.set_finished()

    def use_a_data_block(self, conf, block_pool, oob, datablocktable, lbn):
        blocknum = block_pool.pop_a_free_block_to_data_blocks()
        datablocktable.add_data_block_mapping(lbn=lbn, pbn=blocknum)

        # set all pages to be valid
        start, end = conf.block_to_page_range(blocknum)
        for ppn in range(start, end):
            offset = ppn - start
            lpn = conf.block_off_to_page(lbn, offset)
            oob.states.validate_page(ppn)
            oob.remap(lpn, None, ppn)

        return blocknum


class TestMovingLogBlocks(AssertFinishTestCase):
    def test_init_gc(self):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = create_gc()

        self.set_finished()

    def test_move_data_blocks(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(
            self.proc_test_move_data_blocks(pk))
        simpy_env.run()

    def proc_test_move_data_blocks(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        lpns = range(1, 1 + conf.n_pages_per_block)
        pbn = self.use_a_log_block(conf, oob, block_pool, logmaptable, lpns)

        # we have programmed a whole block
        start, end = conf.block_to_page_range(pbn)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_valid(ppn), True)

        dst_pbn = block_pool.pop_a_free_block_to_log_blocks()
        yield simpy_env.process(gc._move_log_block(pbn, dst_pbn))

        # states bitmap should be in 'erased' state
        start, end = conf.block_to_page_range(pbn)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # oob ppn->lpn mapping should hold nothing
        for ppn in range(start, end):
            with self.assertRaises(KeyError):
                oob.translate_ppn_to_lpn(ppn)

        # new states bitmap should be in 'valid' state,
        # since we have all pages valid in the src pbn
        start, end = conf.block_to_page_range(dst_pbn)
        for ppn in range(start, end):
            self.assertEqual(oob.states.is_page_valid(ppn), True)

        lpns_translated = []
        for ppn in range(start, end):
            lpn = oob.translate_ppn_to_lpn(ppn)
            lpns_translated.append(lpn)
        self.assertListEqual(lpns_translated, lpns)

        # pbn should be free block in block_pool
        self.assertIn(pbn, block_pool.freeblocks)
        self.assertNotIn(pbn, block_pool.log_usedblocks)

        # log block table should hold mapping of dst_pbn
        for i, lpn in enumerate(lpns):
            found, ppn = logmaptable.lpn_to_ppn(lpn)
            block, off = conf.page_to_block_off(ppn)
            self.assertEqual(block, dst_pbn)
            self.assertEqual(off, i)

        # the new physical block should be in log used blocks
        self.assertEqual(len(block_pool.log_usedblocks), 1)

        self.set_finished()

    def use_a_log_block(self, conf, oob, block_pool, logmapping, lpns):
        states = oob.states
        dgn = conf.nkftl_data_group_number_of_lpn(lpns[0])
        for lpn in lpns:
            lpn_dgn = conf.nkftl_data_group_number_of_lpn(lpn)
            assert lpn_dgn == dgn

        cnt = len(lpns)

        # next_ppns_to_program() will allocate block
        ppns = logmapping.next_ppns_to_program(dgn=dgn,
            n=cnt, strip_unit_size='infinity')
        assert len(ppns) == cnt

        pbn, _ = conf.page_to_block_off(ppns[0])

        for lpn, ppn in zip(lpns, ppns):
            oob.remap(lpn, None, ppn)
            logmapping.add_mapping(lpn, ppn)
            ppn_pbn, _ = conf.page_to_block_off(ppn)
            self.assertEqual(ppn_pbn, pbn)

        return pbn


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
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block,
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn = gc.is_switch_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, True)
        self.assertEqual(lbn, 1)


    def test_is_not_switch_mergable_half_used(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn = gc.is_switch_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

    def test_is_not_switch_mergable(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block+1,
                lpn_start=conf.n_pages_per_block+1,
                translator=translator)

        mergable, lbn = gc.is_switch_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

class TestSwitchMerge(AssertFinishTestCase, UseLogBlocksMixin):
    def test_switch_merge(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(
            self.proc_test_switch_merge(pk))
        simpy_env.run()

    def proc_test_switch_merge(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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

        yield simpy_env.process(gc.switch_merge(log_pbn=pbn, logical_block=lbn))

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

        self.set_finished()

class TestPartialMergeMergable(unittest.TestCase, UseLogBlocksMixin):
    def test_is_partial_mergable(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, True)
        self.assertEqual(lbn, 1)
        self.assertEqual(off, int(conf.n_pages_per_block/2))

    def test_is_not_partial_mergable_not_aligned(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=int(conf.n_pages_per_block/2),
                lpn_start=conf.n_pages_per_block+1,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)

    def test_is_not_partial_mergable_because_its_switch_mergable(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        used_blocks = self.use_log_blocks(conf, oob, block_pool,
                logmaptable, cnt=conf.n_pages_per_block,
                lpn_start=conf.n_pages_per_block,
                translator=translator)

        mergable, lbn, off = gc.is_partial_mergable(log_pbn=used_blocks[0])
        self.assertEqual(mergable, False)
        self.assertEqual(lbn, None)


class TestPartialMerge_Merge(AssertFinishTestCase, UseLogBlocksMixin):
    def test_partial_merge(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc_test_partial_merge(pk))
        simpy_env.run()

    def proc_test_partial_merge(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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

        yield simpy_env.process(
            gc.partial_merge(log_pbn=used_blocks[0], lbn=lbn,
            first_free_offset=off))

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

        self.set_finished()

class TestPartialMergeWithMoving(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()


    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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

        yield simpy_env.process(
            gc.partial_merge(log_pbn=pbn, lbn=lbn, first_free_offset=off))

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

        self.set_finished()

class TestFullMerge_Unaligned(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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

        yield simpy_env.process(gc.full_merge(log_pbn=pbn))

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

        self.set_finished()


class TestFullMerge_two_in_two(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        """
        Data of two logical blocks spread in two physical blocks.
        """
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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
        yield simpy_env.process(gc.full_merge(log_pbn=pbn1))

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
        yield simpy_env.process(gc.full_merge(log_pbn=pbn2))

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

        self.set_finished()


class TestFullMerge_with_data_blocks(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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
        yield simpy_env.process(gc.full_merge(log_pbn=log_pbn))

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

        self.set_finished()

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


class TestFullMergeOnePage(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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

        yield simpy_env.process(gc.full_merge(log_pbn=pbn))

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

        self.set_finished()


class TestCleanDataGroup(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        """
        Data of two logical blocks spread in two physical blocks.
        """
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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
        yield simpy_env.process(gc.clean_data_group(
            data_group_no=conf.nkftl_data_group_number_of_logical_block(lbn1)))

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

        self.set_finished()

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
    conf = wiscsim.nkftl2.Config()

    conf['flash_config']['n_pages_per_block'] = 32
    conf['flash_config']['n_blocks_per_plane'] = 64
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 8

    conf['nkftl']['max_blocks_in_log_group'] = 16
    conf['nkftl']['n_blocks_in_data_group'] = 4

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

    loggroup._cur_channel = 0 # override random cur channel

    return blockpool, loggroup

def create_loggroup2_one_channel():
    conf = create_config_1_channel()
    blockpool = NKBlockPool(
            n_channels=conf.n_channels_per_dev,
            n_blocks_per_channel=conf.n_blocks_per_channel,
            n_pages_per_block=conf.n_pages_per_block,
            tags=[TDATA, TLOG])
    loggroup = LogGroup2(
            conf = conf,
            block_pool=blockpool,
            max_n_log_blocks=conf['nkftl']['max_blocks_in_log_group'])

    loggroup._cur_channel = 0 # override random cur channel

    return blockpool, loggroup



class TestLogGroup2(unittest.TestCase):
    def test_next_ppns_in_channel(self):
        blockpool, loggroup = create_loggroup2()

        ppns = loggroup._next_ppns_in_channel(n=1, channel_id=0)

        # it can only get you page when some block has been allocated.
        # So you need to use next_ppns_in_channel_with_allocation
        self.assertEqual(len(ppns), 0)

    def test_next_ppns_in_channel_failure(self):
        blockpool, loggroup = create_loggroup2()
        n_pages_per_block = blockpool.n_pages_per_block

        ppns = loggroup._next_ppns_in_channel(n=n_pages_per_block + 1, channel_id=0)

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

        loggroup._cur_channel = 0
        self.assertEqual(loggroup._cur_channel, 0)
        loggroup._incr_cur_channel()
        self.assertEqual(loggroup._cur_channel, 1)

        for i in range(8):
            ret = loggroup._incr_cur_channel()

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

    def test_next_ppns_infinit_strip(self):
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

        loggroup._cur_channel = 0

        self.assertEqual(loggroup._cur_channel, 0)
        ppns = loggroup.next_ppns(n=1, strip_unit_size=32)
        self.assertEqual(loggroup._cur_channel, 0)
        ppns = loggroup.next_ppns(n=1, strip_unit_size=32)
        self.assertEqual(loggroup._cur_channel, 0)

        ppns = loggroup.next_ppns(n=64, strip_unit_size=32)
        self.assertEqual(len(ppns), 64)
        self.assertEqual(loggroup._cur_channel, 2)

    def test_next_ppns_one_channel(self):
        blockpool, loggroup = create_loggroup2_one_channel()

        loggroup._cur_channel = 0

        self.assertEqual(loggroup._cur_channel, 0)
        ppns = loggroup.next_ppns(n=1, strip_unit_size=32)
        self.assertEqual(loggroup._cur_channel, 0)
        ppns = loggroup.next_ppns(n=1, strip_unit_size=32)
        self.assertEqual(loggroup._cur_channel, 0)

        ppns = loggroup.next_ppns(n=64, strip_unit_size=32)
        self.assertEqual(len(ppns), 64)
        self.assertEqual(loggroup._cur_channel, 0)

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


class TestFTLOperations(AssertFinishTestCase):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_write(env, ftl, conf))
        env.run()

    def proc_test_write(self, env, ftl, conf):
        ext = Extent(lpn_start=1, lpn_count=20)
        yield env.process(ftl.write_ext(ext))

        for lpn in ext.lpn_iter():
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, True)

        self.set_finished()


    def test_read(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_read(env, ftl, conf))
        env.run()

    def proc_test_read(self, env, ftl, conf):
        ext = Extent(lpn_start=1, lpn_count=20)
        yield env.process(ftl.write_ext(ext))

        data = yield env.process(ftl.read_ext(ext))
        self.assertEqual(len(data), ext.lpn_count)

        self.set_finished()

    def test_discard(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_discard(env, ftl, conf))
        env.run()

    def proc_test_discard(self, env, ftl, conf):
        ext = Extent(lpn_start=1, lpn_count=20)
        yield env.process(ftl.write_ext(ext))

        yield env.process(ftl.discard_ext(ext))

        for lpn in ext.lpn_iter():
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

        self.set_finished()

    def test_write_logical_block(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_write_logical_block(env, ftl, conf))
        env.run()

    def proc_test_write_logical_block(self, env, ftl, conf):
        ext = Extent(lpn_start=1, lpn_count=3)
        yield env.process(ftl.write_logical_block(ext))

        ppns = []
        for lpn in ext.lpn_iter():
            found, ppn = ftl.log_mapping_table.lpn_to_ppn(lpn)
            self.assertEqual(ftl.oob.states.is_page_valid(ppn), True)
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            ppns.append(ppn)

        yield env.process(ftl.write_logical_block(ext))

        ppns2 = []
        for lpn in ext.lpn_iter():
            found, ppn, _ = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, True)
            ppns2.append(ppn)

        for ppn1, ppn2 in zip(ppns, ppns2):
            self.assertNotEqual(ppn1, ppn2)

        self.set_finished()

    def test_write_logical_block_overflow(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_write_logical_block_overflow(env, ftl, conf))
        env.run()

    def proc_test_write_logical_block_overflow(self, env, ftl, conf):
        for i in range(3):
            ext = Extent(lpn_start=0, lpn_count=8)
            yield env.process(ftl.write_logical_block(ext))

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

        self.set_finished()

    def test_write_large_extent(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.proc_test_write_large_extent(env, ftl, conf))
        env.run()

    def proc_test_write_large_extent(self, env, ftl, conf):
        for i in range(8):
            ext = Extent(lpn_start=1, lpn_count=conf.n_pages_per_block*63)
            yield env.process(ftl.write_ext(ext))

            ppns = []
            for lpn in ext.lpn_iter():
                found, ppn, loc = ftl.lpn_to_ppn(lpn)
                self.assertEqual(ftl.oob.states.is_page_valid(ppn), True)
                self.assertEqual(found, True)

        self.set_finished()


class TestSimpyIntegration(AssertFinishTestCase, RWMixin):
    def write_proc(self, env, ftl, extent, data=None):
        yield env.process(ftl.write_ext(extent, data))

        self.set_finished()

    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.write_proc(env, ftl, Extent(0, 3)))
        env.run()

    def read_proc(self, env, ftl, extent):
        data = yield env.process(ftl.read_ext(extent))
        self.set_finished()
        env.exit(data)


    def reader_main(self, env, ftl):
        ext = Extent(0, 3)
        yield env.process(
            self.write_proc(env, ftl, ext, self.data_of_extent(ext)))
        ret_data = yield env.process(self.read_proc(env, ftl, ext))
        self.assertListEqual(ret_data, [str(x) for x in ext.lpn_iter()])
        self.set_finished()

    def test_read(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.reader_main(env, ftl))
        env.run()


class TestLogicalBlockSerialization_DifferentLogicalBlock(AssertFinishTestCase, RWMixin):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        p1 = env.process(ftl.write_ext(Extent(0, 1)))
        p2 = env.process(ftl.write_ext(
            Extent(4 * conf.n_pages_per_block, 1)))

        yield simpy.AllOf(env, [p1, p2])

        self.assertTrue(
                env.now == conf.page_prog_time() or
                env.now == 2 * conf.page_prog_time(),
                )

        self.set_finished()

class TestLogicalBlockSerialization_SameLogicalPage(AssertFinishTestCase, RWMixin):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        p1 = env.process(ftl.write_ext(Extent(0, 1)))
        p2 = env.process(ftl.write_ext(Extent(0, 1)))

        yield simpy.AllOf(env, [p1, p2])

        self.assertEqual(env.now, conf.page_prog_time() * 2)

        self.set_finished()


class TestLogicalBlockSerialization_SameLogicalBlock(AssertFinishTestCase, RWMixin):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        p1 = env.process(ftl.write_ext(Extent(0, 1)))
        p2 = env.process(ftl.write_ext(Extent(1, 1)))

        yield simpy.AllOf(env, [p1, p2])

        self.assertEqual(env.now, conf.page_prog_time() * 2)

        self.set_finished()

class TestLogicalBlockSerialization_WriteAndRead2Procs(AssertFinishTestCase, RWMixin):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        extent1 = Extent(0, 1)
        p1 = env.process(ftl.write_ext(extent1,
            self.data_of_extent(extent1)))

        extent2 = Extent(1, 1)
        p2 = env.process(ftl.write_ext(extent2,
            self.data_of_extent(extent2)))

        yield simpy.AllOf(env, [p1, p2])

        ret_data = yield env.process(ftl.read_ext(extent1))
        self.assertListEqual(ret_data, [str(x)
            for x in self.data_of_extent(extent1)])

        ret_data = yield env.process(ftl.read_ext(extent2))
        self.assertListEqual(ret_data, [str(x)
            for x in self.data_of_extent(extent2)])

        self.set_finished()


class TestLogicalBlockSerialization_WriteAndRead(AssertFinishTestCase, RWMixin):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        extent = Extent(0, 1)
        yield env.process(ftl.write_ext(extent,
            self.data_of_extent(extent)))
        ret_data = yield env.process(ftl.read_ext(extent))

        self.assertListEqual(ret_data, [str(x)
            for x in self.data_of_extent(extent)])

        self.assertEqual(env.now, conf.page_prog_time() +
                conf.page_read_time())

        self.set_finished()

class TestLogicalBlockSerialization_Discard(AssertFinishTestCase, RWMixin):
    def test(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        extent = Extent(0, 1)
        yield env.process(ftl.write_ext(extent,
            self.data_of_extent(extent)))

        ret_data = yield env.process(ftl.read_ext(extent))
        self.assertListEqual(ret_data, [str(x)
            for x in self.data_of_extent(extent)])

        yield env.process(ftl.discard_ext(extent))

        # not ppn is valid
        for lpn in extent.lpn_iter():
            found, ppn, loc = ftl.lpn_to_ppn(lpn)
            self.assertEqual(found, False)

        ret_data = yield env.process(ftl.read_ext(extent))
        self.assertListEqual(ret_data, [None
            for x in self.data_of_extent(extent)])

        self.set_finished()

class WriteNCheckMixin(object):
    def write_and_check(self, ftl, extents, env):
        data_mirror = {}
        for ext in extents:
            data = self.random_data(ext)
            self.update_data_mirror(data_mirror, ext, data)
            yield env.process(ftl.write_ext(ext, data))

        for lpn, data in data_mirror.items():
            ret = yield env.process(ftl.lba_read(lpn))
            self.assertEqual(ret, data)

    def update_data_mirror(self, data_mirror, extent, data):
        data_mirror.update(dict(zip(extent.lpn_iter(), data)))

    def random_data(self, extent):
        data = []
        for lpn in extent.lpn_iter():
            d = str(lpn) + '.' + str(random.randint(0, 100))
            data.append(d)
        return data


class TestConcurrency_DataGroupGC(AssertFinishTestCase, WriteNCheckMixin):
    def runTest(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        n = conf.n_pages_per_data_group()
        extents = []
        for i in range(100):
            start = random.randint(0, n-1)
            cnt = random.randint(1, n - start)
            ext = Extent(start, cnt)
            extents.append( ext )
        yield env.process(self.write_and_check(ftl, extents, env))

        self.set_finished()
        print 'end......'


@unittest.skipUnless(TESTALL == True, "Takes too long")
class TestConcurrency_RandomWritesBroad(AssertFinishTestCase, WriteNCheckMixin):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # write different logical_blocks at the same time
        n = int(conf.total_num_pages() * 0.6)
        extents = []
        for i in range(10000):
            start = random.randint(0, n-1)
            cnt = max(1, int(random.randint(1, n - start) / 100))
            ext = Extent(start, cnt)
            extents.append( ext )
        yield env.process(self.write_and_check(ftl, extents, env))

        self.set_finished()


class TestConcurrency_FullMerge(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        """
        Data of two logical blocks spread in two physical blocks.
        """
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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
        p1 = simpy_env.process(gc.full_merge(log_pbn=pbn1))
        p2 = simpy_env.process(gc.full_merge(log_pbn=pbn2))
        yield simpy.AllOf(simpy_env, [p1, p2])

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

        self.set_finished()


class TestConcurrency_cleanlogblock(AssertFinishTestCase, UseLogBlocksMixin):
    def test(self):
        """
        Data of two logical blocks spread in two physical blocks.
        """
        pk = create_gc()

        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

        simpy_env.process(self.proc(pk))
        simpy_env.run()

    def proc(self, pk):
        gc, conf, block_pool, rec, oob, helper, \
        logmaptable, datablocktable, translator, \
        flashobj, simpy_env, des_flash = pk

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
        p1 = simpy_env.process(
            gc.clean_log_block(log_pbn=pbn1, data_group_no=0, tag=""))
        p2 = simpy_env.process(
            gc.clean_log_block(log_pbn=pbn2, data_group_no=0, tag=""))
        yield simpy.AllOf(simpy_env, [p1, p2])

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

        self.set_finished()


class TestConcurrency_WriteNGC(AssertFinishTestCase, WriteNCheckMixin):
    """
    Write a logical space.
    Trigger GC and Write the logical space, the write and gc may
    mess with each other.
    """
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        n = conf.n_pages_per_data_group()
        extents = []
        for i in range(10):
            start = 1 * n + random.randint(0, 2*n-1)
            cnt = random.randint(1, 3 * n - start)
            ext = Extent(start, cnt)
            extents.append( ext )
        yield env.process(self.write_and_check(ftl, extents, env))

        # gc and write
        extents = []
        for i in range(10):
            start = 1 * n + random.randint(0, 2*n-1)
            cnt = random.randint(1, 3 * n - start)
            ext = Extent(start, cnt)
            extents.append( ext )

        p_gc = env.process(ftl.garbage_collector.clean())
        p_write = env.process(self.write_and_check(ftl, extents, env))

        yield simpy.AllOf(env, [p_gc, p_write])

        self.set_finished()


class TestConcurrency_RandomOperations(AssertFinishTestCase):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()
        self.data_mirror = {}

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        for i in range(100):
            # print i
            if i % 1000 == 0:
                print i
            ext = self.random_extent(conf)
            op = self.random_op()
            yield env.process(self.operate(env, ftl, conf, op, ext))

            if i % 1000 == 0:
                yield env.process(ftl.clean(forced=False))

        yield env.process(self.check_mirror(env, ftl, conf))

        self.set_finished()

    def random_extent(self, conf):
        n = int(conf.total_num_pages() * 0.8) # don't use the full logical space
        start = random.randint(0, n-1)
        cnt = max(1, int(random.randint(1, n - start) / 100))
        ext = Extent(start, cnt)
        return ext

    def random_op(self):
        return random.choice(['read', 'write', 'discard'])

    def operate(self, env, ftl, conf, op, extent):
        # print str(extent)
        if op == 'write':
            extent_data = random_data_of_extent(extent)
            self.write_mirror(extent, extent_data)
            yield env.process(ftl.write_ext(extent, extent_data))
        elif op == 'read':
            yield env.process(ftl.read_ext(extent))
        elif op == 'discard':
            self.discard_mirror(extent)
            yield env.process(ftl.discard_ext(extent))

    def write_mirror(self, extent, data):
        for lpn, pagedata in zip(extent.lpn_iter(), data):
            self.data_mirror[lpn] = pagedata

    def discard_mirror(self, extent):
        for lpn in extent.lpn_iter():
            try:
                del self.data_mirror[lpn]
            except KeyError:
                pass

    def check_mirror(self, env, ftl, conf):
        for lpn, data in self.data_mirror.items():
            data_read = yield env.process(
                    ftl.read_ext(Extent(lpn, 1)))
            self.assertEqual(data, data_read[0])


class TestConcurrency_RandomOperationsNCQ(AssertFinishTestCase):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()
        self.data_mirror = {}

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        procs = []
        for i in range(16):
            p = env.process(self.op_proc(env, ftl, conf))
            procs.append(p)
        yield simpy.AllOf(env, procs)

        self.set_finished()

    def op_proc(self, env, ftl, conf):
        for i in range(20):
            # print i
            ext = self.random_extent(conf)
            op = self.random_op()
            yield env.process(self.operate(env, ftl, conf, op, ext))

            if i % 10 == 0:
                yield env.process(ftl.clean(forced=False))

        # yield env.process(self.check_mirror(env, ftl, conf))
    def random_extent(self, conf):
        n = int(conf.total_num_pages() * 0.8) # don't use the full logical space
        start = random.randint(0, n-1)
        cnt = max(1, int(random.randint(1, n - start) / 100))
        ext = Extent(start, cnt)
        return ext

    def random_op(self):
        return random.choice(['read', 'write', 'discard'])

    def operate(self, env, ftl, conf, op, extent):
        # print str(extent)
        if op == 'write':
            extent_data = random_data_of_extent(extent)
            yield env.process(ftl.write_ext(extent, extent_data))
            self.write_mirror(extent, extent_data)
        elif op == 'read':
            yield env.process(ftl.read_ext(extent))
        elif op == 'discard':
            self.discard_mirror(extent)
            yield env.process(ftl.discard_ext(extent))

    def write_mirror(self, extent, data):
        for lpn, pagedata in zip(extent.lpn_iter(), data):
            self.data_mirror[lpn] = pagedata

    def discard_mirror(self, extent):
        for lpn in extent.lpn_iter():
            try:
                del self.data_mirror[lpn]
            except KeyError:
                pass

    def check_mirror(self, env, ftl, conf):
        for lpn, data in self.data_mirror.items():
            data_read = yield env.process(
                    ftl.read_ext(Extent(lpn, 1)))
            self.assertEqual(data, data_read[0])

# @unittest.skip("Take a little too long")
class TestConcurrency_DataIntegrity(AssertFinishTestCase):
    def test_write(self):
        ftl, conf, rec, env = create_nkftl()
        self.data_mirror = {}

        env.process(self.main_proc(env, ftl, conf))
        env.run()

    def main_proc(self, env, ftl, conf):
        # for i in range(8): # use this for longer test
        for i in range(1):
            yield env.process(self.op_and_check_proc(env, ftl, conf))

    def op_and_check_proc(self, env, ftl, conf):
        self.init_non_overlap_exts(conf)

        procs = []
        # for i in range(32): # use this for longer test
        for i in range(4):
            p = env.process(self.op_proc(env, ftl, conf))
            procs.append(p)
        yield simpy.AllOf(env, procs)

        yield env.process(self.check_mirror(env, ftl, conf))

        self.set_finished()

    def op_proc(self, env, ftl, conf):
        while True:
            ext = self.next_ext()
            if ext == None:
                break
            op = self.random_op()
            yield env.process(self.operate(env, ftl, conf, op, ext))

            if i % 10 == 0:
                yield env.process(ftl.clean(forced=False))

    def random_op(self):
        return random.choice(['read', 'write', 'discard'])

    def operate(self, env, ftl, conf, op, extent):
        if op == 'write':
            extent_data = random_data_of_extent(extent)
            yield env.process(ftl.write_ext(extent, extent_data))
            self.write_mirror(extent, extent_data)
        elif op == 'read':
            yield env.process(ftl.read_ext(extent))
        elif op == 'discard':
            self.discard_mirror(extent)
            yield env.process(ftl.discard_ext(extent))

    def write_mirror(self, extent, data):
        for lpn, pagedata in zip(extent.lpn_iter(), data):
            self.data_mirror[lpn] = pagedata

    def discard_mirror(self, extent):
        for lpn in extent.lpn_iter():
            try:
                del self.data_mirror[lpn]
            except KeyError:
                pass

    def check_mirror(self, env, ftl, conf):
        for lpn, data in self.data_mirror.items():
            data_read = yield env.process(
                    ftl.read_ext(Extent(lpn, 1)))
            self.assertEqual(data, data_read[0])

    def init_non_overlap_exts(self, conf):
        self.nonoverlap_exts = self.non_overlap_extents(
                int(conf.total_num_pages() * 0.6), conf.n_pages_per_block)
        random.shuffle(self.nonoverlap_exts)
        self.ext_i = 0

    def next_ext(self):
        try:
            ext = self.nonoverlap_exts[self.ext_i]
        except IndexError:
            return None
        else:
            self.ext_i += 1
            return ext

    def non_overlap_extents(self, n, n_pages_per_block):
        extents = []
        start = 0
        while start < n:
            ideal_size = random.randint(1, n_pages_per_block * 8)
            size = min(ideal_size, n - start)

            ext = Extent(start, size)
            extents.append(ext)

            start += size

        return extents

class TestBlockIter(unittest.TestCase):
    def test_1(self):
        ftl, conf, rec, env = create_nkftl()

        n_pages_per_block = conf.n_pages_per_block

        ret = list(ftl._block_iter_of_extent(Extent(0, 1)))
        self.assertListEqual(ret, [0])

        ret = list(ftl._block_iter_of_extent(Extent(0, n_pages_per_block)))
        self.assertListEqual(ret, [0])

        ret = list(ftl._block_iter_of_extent(Extent(0, n_pages_per_block + 1)))
        self.assertListEqual(ret, [0, 1])

        ret = list(ftl._block_iter_of_extent(
            Extent(n_pages_per_block + 1, 2 * n_pages_per_block)))
        self.assertListEqual(ret, [1, 2, 3])



# Add test without lpn overlap

def main():
    unittest.main()

if __name__ == '__main__':
    main()



