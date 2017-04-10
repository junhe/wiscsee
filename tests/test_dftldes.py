import unittest
import random
import simpy

from wiscsim import ftlsim_commons

from utilities import utils
import wiscsim
from wiscsim.ftlsim_commons import Extent
from wiscsim.dftldes import LpnTable, LpnTableMvpn, UNINITIATED, \
        split_ext_by_segment
from config import WLRUNNER, LBAGENERATOR, LBAMULTIPROC
from commons import *
from utilities.utils import get_expname
import collections
from workflow import run_workflow

class FtlTest(wiscsim.dftldes.Ftl):
    def get_mappings(self):
        return self._mappings

    def get_directory(self):
        return self._directory

    def get_cleaner(self):
        return self._cleaner


def create_config():
    conf = wiscsim.dftldes.Config()
    conf['SSDFramework']['ncq_depth'] = 1

    conf['flash_config']['n_pages_per_block'] = 64
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

    conf['max_victim_valid_ratio'] = 1

    utils.set_exp_metadata(conf, save_data = False,
            expname = 'test_expname',
            subexpname = 'test_subexpname')

    conf['ftl_type'] = 'dftldes'
    conf['simulator_class'] = 'SimulatorDESSync'

    logicsize_mb = 64
    conf.n_cache_entries = conf.n_mapping_entries_per_page
    conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

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

def create_oob(conf):
    oob = wiscsim.dftldes.OutOfBandAreas(conf)
    return oob

def create_blockpool(conf):
    return wiscsim.dftldes.BlockPool(conf)

def create_flashcontrolelr(conf, env, rec):
    return wiscsim.controller.Controller3(env, conf, rec)

def create_simpy_env():
    return simpy.Environment()

def create_translation_directory(conf, oob, block_pool):
    return wiscsim.dftldes.GlobalTranslationDirectory(conf, oob, block_pool)

def create_mapping_on_flash(conf):
    return wiscsim.dftldes.MappingOnFlash(conf)

def create_victimblocks():
    conf = create_config()
    block_pool = create_blockpool(conf)
    oob = create_oob(conf)

    vbs = wiscsim.dftldes.VictimBlocks(conf, block_pool, oob)

    return vbs

def create_wearlevelingvictimblocks():
    conf = create_config()
    block_pool = create_blockpool(conf)
    oob = create_oob(conf)

    vbs = wiscsim.dftldes.WearLevelingVictimBlocks(conf, block_pool, oob, 5)

    return vbs

def create_obj_set(conf):
    rec = create_recorder(conf)
    oob = create_oob(conf)
    block_pool = create_blockpool(conf)
    env = create_simpy_env()
    flash_controller = create_flashcontrolelr(conf, env, rec)
    directory = create_translation_directory(conf, oob, block_pool)
    gmt = create_mapping_on_flash(conf)
    victimblocks = wiscsim.dftldes.VictimBlocks(conf, block_pool, oob)
    trans_page_locks = wiscsim.dftldes.LockPool(env)

    return {'conf':conf, 'rec':rec, 'oob':oob, 'block_pool':block_pool,
            'flash_controller':flash_controller, 'env':env, 'directory':directory,
            'mapping_on_flash':gmt, 'victimblocks':victimblocks,
            'trans_page_locks':trans_page_locks
            }


def create_mapping_cache(objs):
    mapping_cache = wiscsim.dftldes.MappingCache(
            confobj = objs['conf'],
            block_pool = objs['block_pool'],
            flashobj = objs['flash_controller'],
            oobobj = objs['oob'],
            recorderobj = objs['rec'],
            envobj = objs['env'],
            directory = objs['directory'],
            mapping_on_flash = objs['mapping_on_flash'],
            trans_page_locks = objs['trans_page_locks']
            )

    return mapping_cache

def create_datablockcleaner(objs):
    mappings = create_mapping_cache(objs)
    datablockcleaner = wiscsim.dftldes.DataBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = objs['oob'],
            block_pool = objs['block_pool'],
            mappings = mappings,
            rec = objs['rec'],
            env = objs['env'])
    return datablockcleaner


class TestMappingCache(unittest.TestCase):
    def update_m_vpn(self, objs, mapping_cache, m_vpn):
        conf = objs['conf']
        env = objs['env']
        lpns  = conf.m_vpn_to_lpns(m_vpn)
        for lpn in lpns:
            ppn = lpn * 1000
            yield env.process(mapping_cache.update(lpn, ppn))
            ppn_in_cache = yield env.process(mapping_cache.lpn_to_ppn(lpn))
            self.assertEqual(ppn, ppn_in_cache)

    def update_proc(self, objs, mapping_cache):
        conf = objs['conf']
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        yield env.process(self.update_m_vpn(objs, mapping_cache, m_vpn = 3))
        # it should not take any time
        self.assertEqual(env.now, 0)

        yield env.process(self.update_m_vpn(objs, mapping_cache, m_vpn = 4))
        # it should only write one flash page (m_vpn=3) back to flash
        self.assertEqual(env.now, time_program_page)

        lpns  = conf.m_vpn_to_lpns(3)
        lpn = lpns[0]
        ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn))
        self.assertEqual(ppn, lpn * 1000)
        self.assertEqual(env.now, time_program_page + time_program_page + \
                time_read_page)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.update_proc(objs, mapping_cache))
        env.run()


class TestMappingCacheParallel(unittest.TestCase):
    def update_random(self, conf, env, mapping_cache):
        n = conf.total_num_pages()
        print 'n tp pages', conf.total_translation_pages()
        lpns = list(range(n))
        random.shuffle(lpns)
        lpns = lpns[:128]

        procs = []
        mappings = {}
        for lpn in lpns:
            ppn = random.randint(0, 1000)
            mappings[lpn] = ppn
            p = env.process(mapping_cache.update(lpn=lpn, ppn=ppn, tag=None))
            procs.append(p)
        yield simpy.AllOf(env, procs)

        for lpn in lpns:
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, mappings[lpn])

        print "i am finished"

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.update_random(conf, env, mapping_cache))

        env.run()


class TestMappingCacheLoad(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        """
        There are 4 translation pages can be hold in RAM.
        4 MB of data can be mapped.
        translate pages across 4MB of data, translation memory should be full.
        """
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder

        recorder.enable()

        # each tp has 512 entries
        self.assertEqual(lpntable.n_free_rows(), 4*512)

        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, UNINITIATED)

        self.assertEqual(lpntable.n_free_rows(), 0)

        self.assertEqual(recorder.get_count_me('translation',
            'read-trans-for-load'), 4)
        self.assertEqual(recorder.get_count_me('translation',
            'delete-lpn-in-table-for-load'), 0)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()


class TestMappingCacheEviction(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder
        recorder.enable()

        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, UNINITIATED)

        # modify mapping
        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 4)

        # modify mapping out of cache, this should only evict one entry
        # because we only need to insert a new update
        for offset in [4*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 4)
        self.assertEqual(
            recorder.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(
            recorder.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 1)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()


class TestMappingCacheWriteBack(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder
        recorder.enable()

        # load to cache
        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, UNINITIATED)

        # modify all mapping
        for offset in range(0, 4*MB, 2*KB):
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'),
                4*MB/(2*KB))

        # modify mapping out of cache, this should write back a whole TP and
        # and delete one entry in memory
        for offset in [4*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'),
                4*MB/(2*KB))
        self.assertEqual(
            recorder.get_count_me('translation', 'write-back-dirty-for-insert'), 1)
        self.assertEqual(
            recorder.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 1)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()


class TestMappingCacheEvictByTrans(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder
        recorder.enable()

        # load to cache
        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, UNINITIATED)

        # modify all mapping
        for offset in range(0, 4*MB, 2*KB):
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'),
                4*MB/(2*KB))

        # translate a new one
        for offset in [4*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'),
                4*MB/(2*KB))
        self.assertEqual(
            recorder.get_count_me('translation', 'write-back-dirty-for-load'), 1)
        self.assertEqual(
            recorder.get_count_me('translation', 'delete-lpn-in-table-for-load'), 512)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()

class TestMappingCacheFlush(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        """
        There are 4 translation pages can be hold in RAM.
        4 MB of data can be mapped.
        translate pages across 4MB of data, translation memory should be full.
        """
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder

        recorder.enable()

        # each tp has 512 entries
        self.assertEqual(lpntable.n_free_rows(), 4*512)

        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))

        self.assertEqual(lpntable.n_free_rows(), 0)

        yield env.process(mapping_cache.flush())

        for row in lpntable.rows():
            self.assertEqual(row.dirty, False)

        self.assertEqual(len(lpntable.rows()), lpntable._n_rows)

        self.assertEqual(lpntable.n_free_rows(), 0)
        mapping_cache.drop()
        self.assertEqual(lpntable.n_free_rows(), 4*512)

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 0)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()

class TestMappingCacheFlushWithEviction(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder
        recorder.enable()

        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, UNINITIATED)

        # modify mapping
        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 4)

        yield env.process(mapping_cache.flush())

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 4)
        self.assertEqual(
            recorder.get_count_me('translation', 'write-back-dirty-for-flush'), 4)

        mapping_cache.drop()
        self.assertEqual(
            recorder.get_count_me('translation', 'delete-lpn-in-table-for-drop'), 512*4)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()


class TestMappingCacheFlushAndLoad(unittest.TestCase):
    def load(self, conf, env, mapping_cache):
        lpntable = mapping_cache._lpn_table
        recorder = mapping_cache.recorder
        recorder.enable()

        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, UNINITIATED)

        self.assertEqual(recorder.get_count_me('translation',
            'read-trans-for-load'), 4)

        # modify mapping
        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.update(lpn=lpn, ppn=lpn+1, tag=None))

        self.assertEqual(recorder.get_count_me('translation',
            'read-trans-for-load'), 4)
        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 4)

        yield env.process(mapping_cache.flush())

        self.assertEqual(
            recorder.get_count_me('translation', 'overwrite-in-cache'), 4)
        self.assertEqual(
            recorder.get_count_me('translation', 'write-back-dirty-for-flush'), 4)

        mapping_cache.drop()
        self.assertEqual(
            recorder.get_count_me('translation', 'delete-lpn-in-table-for-drop'), 512*4)

        for offset in [0*MB, 1*MB, 2*MB, 3*MB]:
            lpn = offset / (2*KB)
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, lpn+1) #stored previously

        self.assertEqual(recorder.get_count_me('translation',
            'read-trans-for-load'), 4+4)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.load(conf, env, mapping_cache))

        env.run()




class TestMappingCacheSimpleTranslation(unittest.TestCase):
    def translate(self, conf, env, mapping_cache):
        ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=0, tag=None))
        self.assertEqual(ppn, UNINITIATED)
        env.exit('finished')

    def runme(self, conf, env, mapping_cache):
        ret = yield env.process(self.translate(conf, env, mapping_cache))
        self.assertEqual(ret, 'finished')

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.runme(conf, env, mapping_cache))
        env.run()


class TestMappingCacheParallel3(unittest.TestCase):
    def update_and_check(self, conf, env, mapping_cache):
        n = conf.total_num_pages()
        lpns = list(range(n))
        random.shuffle(lpns)
        lpns = lpns[:128]

        procs = []
        mappings = {}
        for lpn in lpns:
            ppn = random.randint(0, 1000)
            mappings[lpn] = ppn
            p = env.process(mapping_cache.update(lpn=lpn, ppn=ppn, tag=None))
            procs.append(p)

        procs = []
        translate_lpns = list(range(n))
        random.shuffle(translate_lpns)
        translate_lpns = translate_lpns[:128]
        for lpn in translate_lpns:
            p = env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            procs.append(p)

        yield simpy.AllOf(env, procs)

        procs = []
        for lpn in lpns:
            ppn = yield env.process(mapping_cache.lpn_to_ppn(lpn=lpn, tag=None))
            self.assertEqual(ppn, mappings[lpn])

        print 'finished'

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.update_and_check(conf, env, mapping_cache))

        env.run()


class TestMappingCacheSameLpnTranslation(unittest.TestCase):
    def translate(self, conf, env, mapping_cache):
        p1 = env.process(mapping_cache.lpn_to_ppn(lpn=0, tag=None))
        p2 = env.process(mapping_cache.lpn_to_ppn(lpn=0, tag=None))
        yield simpy.AllOf(env, [p1, p2])
        print 'finished'

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.translate(conf, env, mapping_cache))
        env.run()


class TestMappingCacheSameLpnUpdate(unittest.TestCase):
    def translate(self, conf, env, mapping_cache):
        p1 = env.process(mapping_cache.update(lpn=0, ppn=1, tag=None))
        p2 = env.process(mapping_cache.update(lpn=0, ppn=2, tag=None))
        yield simpy.AllOf(env, [p1, p2])
        print 'finished'

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.translate(conf, env, mapping_cache))
        env.run()


@unittest.skipUnless(TESTALL == True, "Skip unless we want to test all")
class TestMappingCacheSameLpnUpdateWEvict(unittest.TestCase):
    def translate(self, conf, env, mapping_cache):
        # dirty 4 translation pages to mem
        n = conf.n_mapping_entries_per_page
        ext = Extent(lpn_start=0, lpn_count=4*n)
        for lpn in ext.lpn_iter():
            yield env.process(mapping_cache.update(lpn=lpn, ppn=3))

        # update a lpn that is not in mem
        ext = Extent(lpn_start=4*n, lpn_count=4*n)
        procs = []
        for lpn in ext.lpn_iter():
            p = env.process(mapping_cache.update(lpn=lpn, ppn=1, tag=None))
            procs.append(p)
        yield simpy.AllOf(env, procs)
        print 'finished'

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 4
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.translate(conf, env, mapping_cache))
        env.run()


class TestParallelTranslation(unittest.TestCase):
    def write_proc(self, env, dftl, extent):
        yield env.process(dftl.write_ext(extent))

    def read_proc(self, env, dftl, extent):
        yield env.process(dftl.read_ext(extent))

    def access_same_vpn(self, env, dftl):
        # assert count of read and write

        # write data page ->
        writer1 = env.process(self.write_proc(env, dftl, ext))
        writer2 = env.process(self.write_proc(env, dftl, ext))

        yield env.events.AllOf([writer1, writer2])

        self.assertEqual(tp_read_count, 1)
        self.assertEqual(env.now, read + write)

    def access_different_vpn(self, env, dftl):
        # assert count of read and write
        reader = env.process(self.write_proc(env, dftl, ext1))
        writer = env.process(self.write_proc(env, dftl, ext2))

        yield env.events.AllOf([reader, writer])

        self.assertEqual(tp_read_count, 2)
        self.assertEqual(env.now, read + write)

    def test_parallel_translation(self):
        conf = create_config()
        objs = create_obj_set(conf)

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])


    def test_write(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

    def proc_test_write(self, env, dftl):
        pass

class TestWrite(unittest.TestCase):
    def test_write(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl, Extent(0, 1)))
        env.run()

    def proc_test_write(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        rec = objs['rec']
        rec.enable()

        yield env.process(dftl.write_ext(ext))

        # read translation page and write data page in the same time
        self.assertEqual(env.now, max(time_read_page, time_program_page))
        self.assertEqual(rec.get_count_me("Mapping_Cache", "hit"), 0)
        self.assertEqual(rec.get_count_me("Mapping_Cache", "miss"), 1)

    def test_write_larger(self):
        conf = create_config()
        conf['stripe_size'] = 1

        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write_larger(objs, dftl, Extent(0, 2)))
        env.run()

    def proc_test_write_larger(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        rec = objs['rec']
        rec.enable()

        yield env.process(dftl.write_ext(ext))

        # writing 2 data page and reading trans page happens at the same time
        self.assertEqual(env.now, max(time_read_page, time_program_page))

        self.assertEqual(rec.get_count_me("Mapping_Cache", "miss"), 1)
        # The second lpn is a hit
        self.assertEqual(rec.get_count_me("Mapping_Cache", "hit"), 1)

    def test_write_larger2(self):
        conf = create_config()
        conf['stripe_size'] = 1
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write_larger2(objs, dftl,
            Extent(0, 4)))
        env.run()

    def proc_test_write_larger2(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        rec = objs['rec']
        rec.enable()

        yield env.process(dftl.write_ext(ext))

        # writing 4 data page and reading 1 trans page at the same time,
        # since there are only 4 channels, 1 trans read and one data page
        # write compete for one channel
        self.assertEqual(env.now, time_read_page + time_program_page)

        self.assertEqual(rec.get_count_me("Mapping_Cache", "miss"), 1)
        self.assertEqual(rec.get_count_me("Mapping_Cache", "hit"), 3)

    def test_write_2vpn(self):
        conf = create_config()
        conf['stripe_size'] = 1
        # make sure no cache miss in this test
        conf.n_cache_entries = conf.n_mapping_entries_per_page * 100

        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write_2vpn(objs, dftl,
            Extent(0, conf.n_mapping_entries_per_page * 2)))
        env.run()

    def proc_test_write_2vpn(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        rec = objs['rec']
        rec.enable()

        yield env.process(dftl.write_ext(ext))

        # read translation page, then write all 4 data pages at the same time
        self.assertEqual(env.now, time_read_page +
                (ext.lpn_count/4) * time_program_page)

        self.assertEqual(rec.get_count_me("Mapping_Cache", "miss"), 2)
        self.assertEqual(rec.get_count_me("Mapping_Cache", "hit"),
                objs['conf'].n_mapping_entries_per_page * 2 - 2)

    def test_write_outofspace(self):
        conf = create_config()
        conf['stripe_size'] = 1
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write_outofspace(objs, dftl,
            Extent(0, conf.total_num_pages())))
        env.run()

    def proc_test_write_outofspace(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        with self.assertRaises(wiscsim.blkpool.OutOfSpaceError):
            yield env.process(dftl.write_ext(ext))


class TestRead(unittest.TestCase):
    def test_read(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_read(objs, dftl, Extent(0, 1)))
        env.run()

    def proc_test_read(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        yield env.process(dftl.read_ext(ext))

        # only read translation page from flash
        # data page is not read since it is not initialized
        self.assertEqual(env.now, time_read_page)

    def test_read_larger(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        # read a whole m_vpn's lpn
        env.process(self.proc_test_read_larger(objs, dftl,
            Extent(0, conf.n_mapping_entries_per_page)))
        env.run()

    def proc_test_read_larger(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        yield env.process(dftl.read_ext(ext))

        # only read one translation page from flash
        # data page is not read since it is not initialized
        self.assertEqual(env.now, time_read_page)

    def _test_read_2tp(self):
        """
        This test fails because there is cache thrashing problem
        The solution should be that we prevent the translation page to be
        evicted while translating for a mvpn
        """
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        # read two m_vpn's lpns
        env.process(self.proc_test_read_2tp(objs, dftl,
            Extent(0, 2 * conf.n_mapping_entries_per_page)))
        env.run()

    def proc_test_read_2tp(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        yield env.process(dftl.read_ext(ext))

        # read two translation page from flash,
        # at this time they are serialized
        # data page is not read since it is not initialized
        self.assertEqual(env.now, time_read_page * 2)


class TestSplit(unittest.TestCase):
    def test(self):
        conf = create_config()
        n = conf.n_mapping_entries_per_page

        result = wiscsim.dftldes.split_ext_to_mvpngroups(conf, Extent(0, n))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].lpn_start, 0)
        self.assertEqual(result[0].end_lpn(), n)

        result = wiscsim.dftldes.split_ext_to_mvpngroups(conf, Extent(0, n + 1))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].lpn_start, 0)
        self.assertEqual(result[0].end_lpn(), n)
        self.assertEqual(result[1].lpn_start, n)
        self.assertEqual(result[1].end_lpn(), n + 1)


class TestDiscard(unittest.TestCase):
    def test_discard(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_discard(objs, dftl, Extent(0, 1)))
        env.run()

    def proc_test_discard(self, objs, dftl, ext):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        yield env.process(dftl.discard_ext(ext))

        # need to read TP, then we mark the entry as UNINITIATED in mem
        # (the entry was UNINITIATED before)
        self.assertEqual(env.now, time_read_page)

        # it takes no time since the page is UNINITIATED
        yield env.process(dftl.read_ext(ext))
        self.assertEqual(env.now, time_read_page)

        # the mapping is in cache, you just need to write data page
        yield env.process(dftl.write_ext(ext))
        self.assertEqual(env.now, time_read_page + time_program_page)

        # now you need to really read a flash page as it is inintialized
        yield env.process(dftl.read_ext(ext))
        self.assertEqual(env.now, 2 * time_read_page + time_program_page)

        # discarding takes no time as mapping is in memory
        yield env.process(dftl.discard_ext(ext))
        self.assertEqual(env.now, 2 * time_read_page + time_program_page)


class TestLpnTable(unittest.TestCase):
    def test_init(self):
        table = LpnTable(8)
        self.assertEqual(table.n_free_rows(), 8)
        self.assertEqual(table.n_locked_free_rows(), 0)
        self.assertEqual(table.n_used_rows(), 0)

    def test_ops(self):
        """
        lock before adding, also you need to tell it which row you add to
        """
        table = LpnTable(8)

        rowid = table.lock_free_row()
        self.assertEqual(table.n_free_rows(), 7)
        self.assertEqual(table.n_locked_free_rows(), 1)
        self.assertEqual(table.n_used_rows(), 0)

        table.add_lpn(rowid = rowid, lpn = 8, ppn = 88, dirty = True)
        self.assertEqual(table.lpn_to_ppn(8), 88)
        self.assertEqual(table.is_dirty(8), True)
        self.assertEqual(table.n_free_rows(), 7)
        self.assertEqual(table.n_locked_free_rows(), 0)
        self.assertEqual(table.n_used_rows(), 1)

        table.overwrite_lpn(lpn = 8, ppn = 89, dirty = False)
        self.assertEqual(table.lpn_to_ppn(8), 89)
        self.assertEqual(table.is_dirty(8), False)
        self.assertEqual(table.n_free_rows(), 7)
        self.assertEqual(table.n_locked_free_rows(), 0)
        self.assertEqual(table.n_used_rows(), 1)

        deleted_rowid = table.delete_lpn_and_lock(lpn = 8)
        self.assertEqual(rowid, deleted_rowid)
        self.assertEqual(table.has_lpn(8), False)
        self.assertEqual(table.n_free_rows(), 7)
        self.assertEqual(table.n_locked_free_rows(), 1)
        self.assertEqual(table.n_used_rows(), 0)

        table.unlock_free_row(rowid = deleted_rowid)
        self.assertEqual(table.n_free_rows(), 8)
        self.assertEqual(table.n_locked_free_rows(), 0)
        self.assertEqual(table.n_used_rows(), 0)

    def test_boundaries(self):
        table = LpnTable(8)

        for i in range(8):
            table.lock_free_row()

        self.assertEqual(table.lock_free_row(), None)

    def test_multiple_adds(self):
        table = LpnTable(8)

        locked_rows = table.lock_free_rows(3)
        self.assertEqual(len(locked_rows), 3)

        table.add_lpns(locked_rows, {1:11, 2:22, 3:33}, False)

        self.assertEqual(table.n_free_rows(), 5)
        self.assertEqual(table.n_locked_free_rows(), 0)
        self.assertEqual(table.n_used_rows(), 3)

        self.assertEqual(table.lpn_to_ppn(1), 11)
        self.assertEqual(table.lpn_to_ppn(2), 22)
        self.assertEqual(table.lpn_to_ppn(3), 33)

    def test_locking_lpn(self):
        table = LpnTable(8)

        locked_rows = table.lock_free_rows(3)
        self.assertEqual(len(locked_rows), 3)
        self.assertListEqual(locked_rows, [0,1,2])

        table.add_lpns(locked_rows, {1:11, 2:22, 3:33}, False)

        table.lock_lpn(1)
        self.assertEqual(table.n_free_rows(), 5)
        self.assertEqual(table.n_locked_free_rows(), 0)
        self.assertEqual(table.n_used_rows(), 2)
        self.assertEqual(table.n_locked_used_rows(), 1)


class TestLockPool(unittest.TestCase):
    def access_vpn(self, env, respool, vpn):
        req = respool.get_request(vpn)
        yield req
        yield env.timeout(10)
        respool.release_request(vpn, req)

    def init_proc(self, env, respool):
        procs = []
        for i in range(3):
            p = env.process(self.access_vpn(env, respool, 88))
            procs.append(p)

            p = env.process(self.access_vpn(env, respool, 89))
            procs.append(p)

        yield simpy.events.AllOf(env, procs)
        self.assertEqual(env.now, 30)

    def test_request(self):
        env = simpy.Environment()
        respool = wiscsim.dftldes.LockPool(env)

        env.process(self.init_proc(env, respool))
        env.run()


class TestParallelDFTL(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftldes.Config()
        self.conf['SSDFramework']['ncq_depth'] = 1

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4

    def setup_environment(self):
        metadata_dic = utils.choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD

        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkloadFLEX2"
        self.conf["lba_workload_configs"]["ExtentTestWorkloadFLEX2"] = {
                "events": [
                    (d, 1, 3),
                    (w, 1, 3),
                    (r, 1, 3)
                    ]}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDESNew'

        logicsize_mb = 2
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page \
                * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def my_run(self):
        utils.runtime_update(self.conf)
        run_workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestFTLwithMoreData(unittest.TestCase):
    def setup_config(self):
        self.conf = wiscsim.dftldes.Config()
        self.conf['SSDFramework']['ncq_depth'] = 2

        self.conf['flash_config']['page_size'] = 2048
        self.conf['flash_config']['n_pages_per_block'] = 4
        self.conf['flash_config']['n_blocks_per_plane'] = 8
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 4


    def setup_environment(self):
        metadata_dic = utils.choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "TestWorkloadFLEX3"

        traffic = 2*MB
        chunk_size = 32*KB
        page_size = self.conf['flash_config']['page_size']
        self.conf["lba_workload_configs"]["TestWorkloadFLEX3"] = {
                "op_count": traffic/chunk_size,
                "extent_size": chunk_size/page_size ,
                "ops": [OP_WRITE], 'mode': 'random'}
        print self.conf['lba_workload_configs']['TestWorkloadFLEX3']
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDESNew'

        logicsize_mb = 8
        entries_need = int(logicsize_mb * 2**20 * 0.03 / self.conf['flash_config']['page_size'])
        self.conf.mapping_cache_bytes = 4 * self.conf.n_mapping_entries_per_page * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.3))
        print "Current n_blocks_per_plane",\
            self.conf['flash_config']['n_blocks_per_plane']

    def my_run(self):
        utils.runtime_update(self.conf)
        run_workflow(self.conf)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestTranslationWithWrite(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 1

        conf.n_cache_entries = 4*conf.n_mapping_entries_per_page
        conf.set_flash_num_blocks_by_bytes(128*MB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob
        conf = objs['conf']
        PAGE_SIZE = 2*KB

        # Write first 4*MB
        for i in range(4):
            yield env.process(dftl.write_ext(
                Extent((i*MB/PAGE_SIZE), 1*MB/PAGE_SIZE)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0)
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-load'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'read-trans-for-load'), 4)
        self.assertEqual(rec.get_count_me('translation', 'overwrite-in-cache'), 4*512)
        self.assertEqual(rec.get_count_me('translation', 'insert-to-free'), 0)

        yield env.process(dftl.write_ext(Extent(4*MB/PAGE_SIZE, 1)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0+1)
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-load'), 0+512)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 0+0)
        self.assertEqual(rec.get_count_me('translation', 'read-trans-for-load'), 4+1)
        self.assertEqual(rec.get_count_me('translation', 'overwrite-in-cache'), 4*512+1)
        self.assertEqual(rec.get_count_me('translation', 'insert-to-free'), 0+0)

        yield env.process(dftl.write_ext(Extent(5*MB/PAGE_SIZE, 1)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0+1+1)
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-load'), 0+512+512)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 0+0+0)
        self.assertEqual(rec.get_count_me('translation', 'read-trans-for-load'), 4+1+1)
        self.assertEqual(rec.get_count_me('translation', 'overwrite-in-cache'), 4*512+1+1)
        self.assertEqual(rec.get_count_me('translation', 'insert-to-free'), 0+0+0)


class TestTranslationWithWriteTwice(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 1

        conf.n_cache_entries = 4*conf.n_mapping_entries_per_page
        conf.set_flash_num_blocks_by_bytes(128*MB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob
        conf = objs['conf']
        PAGE_SIZE = 2*KB

        # Write first 4*MB
        for i in range(4):
            yield env.process(dftl.write_ext(
                Extent((i*MB/PAGE_SIZE), 1*MB/PAGE_SIZE)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0)

        for i in range(4, 8):
            yield env.process(dftl.write_ext(
                Extent((i*MB/PAGE_SIZE), 1*MB/PAGE_SIZE)))

        # should have purged previous mappings
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 4)

        for i in range(4):
            yield env.process(dftl.write_ext(
                Extent((i*MB/PAGE_SIZE), 1*MB/PAGE_SIZE)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 8)


class TestTranslationWriteBacks(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 1

        conf.n_cache_entries = 4*conf.n_mapping_entries_per_page
        conf.set_flash_num_blocks_by_bytes(1*GB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        return
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob
        conf = objs['conf']
        PAGE_SIZE = 2*KB

        n = 4*512 - 511
        npages = 32*MB/PAGE_SIZE
        lpns = range(npages)
        # random.shuffle(lpns)

        print n
        for i in range(4*512*2):
            lpn = random.choice(lpns)
            yield env.process(dftl.write_ext(Extent(lpn, 1)))
            # yield env.process(dftl.write_ext(Extent(lpns[i], 1)))
            print i, n,  rec.get_count_me('translation', 'write-back-dirty-for-load')

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0)




class TestTranslationWithWriteNotAligned(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 1

        conf.n_cache_entries = 4*conf.n_mapping_entries_per_page
        conf.set_flash_num_blocks_by_bytes(128*MB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob
        conf = objs['conf']
        PAGE_SIZE = 2*KB

        # Write 4 places
        for i in [8, 22, 33, 77]:
            yield env.process(dftl.write_ext(
                Extent((i*MB/PAGE_SIZE) + 1, 1)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0)
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-load'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'read-trans-for-load'), 4)
        self.assertEqual(rec.get_count_me('translation', 'overwrite-in-cache'), 4)
        self.assertEqual(rec.get_count_me('translation', 'insert-to-free'), 0)

        yield env.process(dftl.write_ext(Extent(7*MB/PAGE_SIZE + 1, 1)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0+0)
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-load'), 0+512)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 0+0)
        self.assertEqual(rec.get_count_me('translation', 'read-trans-for-load'), 4+1)
        self.assertEqual(rec.get_count_me('translation', 'overwrite-in-cache'), 4+1)
        self.assertEqual(rec.get_count_me('translation', 'insert-to-free'), 0+0)

        yield env.process(dftl.write_ext(Extent(5*MB/PAGE_SIZE, 1)))

        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-load'), 0)
        self.assertEqual(rec.get_count_me('translation', 'write-back-dirty-for-insert'), 0)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-load'), 0+512+512)
        self.assertEqual(rec.get_count_me('translation', 'delete-lpn-in-table-for-insert'), 0+0+0)
        self.assertEqual(rec.get_count_me('translation', 'read-trans-for-load'), 4+1+1)
        self.assertEqual(rec.get_count_me('translation', 'overwrite-in-cache'), 4+1+1)
        self.assertEqual(rec.get_count_me('translation', 'insert-to-free'), 0+0+0)


class TestWearLevelingVictimBlocks(unittest.TestCase):
    def test_entry(self):
        wiscsim.dftldes.WearLevelingVictimBlocks

    def test_init(self):
        create_wearlevelingvictimblocks()

    def test_empty(self):
        vbs = create_wearlevelingvictimblocks()
        self.assertEqual(len(list(vbs.iterator_verbose())), 0)

    def test_some_used(self):
        vbs = create_wearlevelingvictimblocks()
        vbs.n_victims = 2
        blockpool = vbs._block_pool

        # use 3
        block1 = blockpool.pop_a_free_block_to_data()
        block2 = blockpool.pop_a_free_block_to_data()
        block3 = blockpool.pop_a_free_block_to_data()
        blockpool.move_used_data_block_to_free(block1)
        blockpool.move_used_data_block_to_free(block2)
        blockpool.move_used_data_block_to_free(block3)

        # victim blocks are used
        block1 = blockpool.pop_a_free_block_to_data()
        block2 = blockpool.pop_a_free_block_to_data()
        block3 = blockpool.pop_a_free_block_to_data()

        self.assertEqual(len(list(vbs.iterator_verbose())), 2)

        vbs.n_victims = 3
        victims = list(vbs.iterator_verbose())
        vblocks = [ blocknum for _, _, blocknum in victims ]
        self.assertListEqual(sorted(vblocks), sorted([block1, block2, block3]))


class TestVictimBlocks(unittest.TestCase):
    def test_entry(self):
        wiscsim.dftldes.VictimBlocks

    def test_init(self):
        create_victimblocks()

    def test_empty(self):
        vbs = create_victimblocks()
        self.assertEqual(len(list(vbs.iterator())), 0)

    def test_cur_blocks(self):
        """
        cur blocks should not be victim
        """
        conf = create_config()
        block_pool = create_blockpool(conf)
        oob = create_oob(conf)

        vbs = wiscsim.dftldes.VictimBlocks(conf, block_pool, oob)

        ppn = block_pool.next_data_page_to_program()

        self.assertEqual(len(list(vbs.iterator())), 0)

    def test_one_victim_candidate(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        block_pool = create_blockpool(conf)
        oob = create_oob(conf)

        # use a whole block
        n = conf.n_pages_per_block
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        oob.invalidate_ppns(ppns)

        # use one more
        ppns = block_pool.next_n_data_pages_to_program_striped(1)

        vbs = wiscsim.dftldes.VictimBlocks(conf, block_pool, oob)

        victims = list(vbs.iterator())
        self.assertEqual(len(victims), 1)

    def test_3_victim_candidates(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        block_pool = create_blockpool(conf)
        oob = create_oob(conf)

        # invalidate n-2 pages
        n = conf.n_pages_per_block
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        block0, _ = conf.page_to_block_off(ppns[0])
        for ppn in ppns:
            oob.states.validate_page(ppn)
        oob.invalidate_ppns(ppns[:-2])

        # invalidate n-1 pages
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        block1, _ = conf.page_to_block_off(ppns[0])
        for ppn in ppns:
            oob.states.validate_page(ppn)
        oob.invalidate_ppns(ppns[:-1])

        # invalidate n-3 pages
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        block2, _ = conf.page_to_block_off(ppns[0])
        for ppn in ppns:
            oob.states.validate_page(ppn)
        oob.invalidate_ppns(ppns[:-3])

        # use one more
        ppns = block_pool.next_n_data_pages_to_program_striped(1)

        vbs = wiscsim.dftldes.VictimBlocks(conf, block_pool, oob)

        victims = list(vbs.iterator())

        self.assertListEqual(victims, [block1, block0, block2])

    def test_valid_ratio_stats(self):
        vbs = create_victimblocks()
        conf = vbs._conf
        blockpool = vbs._block_pool
        oob = vbs._oob

        n = conf.n_pages_per_block
        ratios = []
        self.validate_n_pages_in_block(conf, blockpool, oob, 0)
        self.validate_n_pages_in_block(conf, blockpool, oob, 0)
        ratios.append( 0 / n )
        self.validate_n_pages_in_block(conf, blockpool, oob, 2)
        self.validate_n_pages_in_block(conf, blockpool, oob, 2)
        ratios.append( 2 / n )
        self.validate_n_pages_in_block(conf, blockpool, oob, 3)
        self.validate_n_pages_in_block(conf, blockpool, oob, 3)
        ratios.append( 3 / n )
        self.validate_n_pages_in_block(conf, blockpool, oob, n)
        self.validate_n_pages_in_block(conf, blockpool, oob, n)
        ratios.append( n / n )

        counter = vbs.get_valid_ratio_counter_of_used_blocks()
        for valid_ratio in ratios:
            ratio_str = "{0:.2f}".format(valid_ratio)
            self.assertEqual(counter[ratio_str], 2)

    def validate_n_pages_in_block(self, conf, block_pool, oob, n):
        blocknum = block_pool.pop_a_free_block_to_data()
        for i in range(n):
            ppn = conf.block_off_to_page(blocknum, i)
            oob.states.validate_page(ppn)



class TestGC(unittest.TestCase):
    def test_valid_ratio(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        env = objs['env']
        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time

        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)


        conf = objs['conf']
        n = conf.n_pages_per_block
        yield env.process(dftl.write_ext(Extent(0, n)))
        self.assertEqual(len(list(victims.iterator_verbose())), 0)

        yield env.process(dftl.write_ext(Extent(0, 1)))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, block_num = victim_blocks[0]
        self.assertEqual(valid_ratio, (n-1.0)/n)

        yield env.process(dftl.write_ext(Extent(0, n)))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, block_num = victim_blocks[0]
        self.assertEqual(valid_ratio, 0)


class TestDataBlockCleaner(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)
        datablockcleaner = wiscsim.dftldes.DataBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            rec = objs['rec'],
            env = objs['env'])

        n = conf.n_pages_per_block
        yield env.process(dftl.write_ext(Extent(0, n)))
        yield env.process(dftl.write_ext(Extent(0, 1)))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, victim_block = victim_blocks[0]
        self.assertEqual(oob.states.block_valid_ratio(victim_block), (n-1.0)/n)

        s = env.now
        yield env.process(datablockcleaner.clean(victim_block))

        # check the time to move n-1 valid pages
        self.assertEqual(env.now,
                s + (n-1)*(time_read_page+time_program_page) + time_erase_block)

        # check validation
        self.assertEqual(oob.states.block_valid_ratio(victim_block), 0)
        ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        for ppn in range(ppn_s, ppn_e):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        self.assertNotIn(victim_block, block_pool.current_blocks())
        self.assertNotIn(victim_block, block_pool.used_blocks)
        self.assertIn(victim_block, block_pool.freeblocks)

        # check mapping
        ppn = yield env.process(mappings.lpn_to_ppn(0))
        block, _ = conf.page_to_block_off(ppn)
        self.assertNotEqual(block, victim_block)


class TestTransBlockCleaner(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        # let cache just enough to hold one translation page
        conf.n_cache_entries = conf.n_mapping_entries_per_page
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()


    def evict(self, env, mappings, lpn0, lpn1, n_evictions):
        """
        lpn0 and lpn1 are in two different m_vpns
        try to get n_evictions
        """
        yield env.process(mappings.lpn_to_ppn(lpn0))
        yield env.process(mappings.update(lpn=lpn0, ppn=8))

        lpns = [lpn1, lpn0] * n_evictions
        lpns = lpns[:n_evictions]
        for (i, lpn) in enumerate(lpns):
            # every iteration will evict one translation page
            yield env.process(mappings.lpn_to_ppn(lpn))
            yield env.process(mappings.update(lpn=lpn, ppn=i))


    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        directory = dftl.get_directory()

        # there should be a current translation block
        self.assertGreater(len(block_pool.current_blocks()), 0)

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)
        transblockcleaner = wiscsim.dftldes.TransBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            directory = objs['directory'],
            rec = objs['rec'],
            env = objs['env'],
            trans_page_locks = objs['trans_page_locks']
            )

        k = conf.n_mapping_entries_per_page
        n = conf.n_pages_per_block
        n_tp = conf.total_translation_pages()

        # try to evict many so we have a used translation block
        yield env.process(self.evict(env, mappings, 0, k, n/2))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, victim_block = victim_blocks[0]

        self.assertEqual(valid_ratio,
                (min(n_tp, conf.n_pages_per_block) - 2.0) / n)

        s = env.now
        yield env.process(transblockcleaner.clean(victim_block))

        # check the time to move n-1 valid pages
        self.assertEqual(env.now,
                s + (min(n_tp, conf.n_pages_per_block)-2)*\
                (time_read_page+time_program_page) + time_erase_block)

        # check validation
        self.assertEqual(oob.states.block_valid_ratio(victim_block), 0)
        ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        for ppn in range(ppn_s, ppn_e):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        self.assertNotIn(victim_block, block_pool.current_blocks())
        self.assertNotIn(victim_block, block_pool.used_blocks)
        self.assertIn(victim_block, block_pool.freeblocks)

        # check directory
        self.assertNotEqual(conf.page_to_block_off(directory.m_vpn_to_m_ppn(0))[0],
                victim_block)

class TestGlobalGC(unittest.TestCase):
    def test_used_ratio(self):
        conf = create_config()
        block_pool = create_blockpool(conf)

        n = conf.n_blocks_per_dev

        self.assertEqual(block_pool.used_ratio(), 0)

        # use one block
        ppn = block_pool.next_n_data_pages_to_program_striped(1)

        self.assertEqual(block_pool.used_ratio(), 1.0/n)

    def test_threshold_sanity(self):
        conf = create_config()
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_high_threshold_ratio = 0.9
        conf.GC_low_threshold_ratio = 0.5
        conf.over_provisioning = 1.28

        objs = create_obj_set(conf)
        env = objs['env']
        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

    def test_threshold_sanity2(self):
        conf = create_config()
        conf.set_flash_num_blocks_by_bytes(1*MB)
        conf.GC_high_threshold_ratio = 0.9
        conf.GC_low_threshold_ratio = 0.5
        conf.over_provisioning = 1.28

        objs = create_obj_set(conf)
        env = objs['env']
        with self.assertRaisesRegexp(RuntimeError, 'spare blocks'):
            dftl = FtlTest(objs['conf'], objs['rec'],
                    objs['flash_controller'], objs['env'])

    def test_gc_triggering(self):
        conf = create_config()
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_high_threshold_ratio = 0.9
        conf.GC_low_threshold_ratio = 0.5
        objs = create_obj_set(conf)
        env = objs['env']
        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])
        # cleaner = dftl.get_cleaner()


class TestCleaningDataBlocksByCleaner(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_low_threshold_ratio = 0
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        cleaner = dftl.get_cleaner()

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)
        datablockcleaner = wiscsim.dftldes.DataBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            rec = objs['rec'],
            env = objs['env'])

        n = conf.n_pages_per_block
        yield env.process(dftl.write_ext(Extent(0, n)))
        old_ppn = yield env.process(mappings.lpn_to_ppn(lpn=0))
        yield env.process(dftl.write_ext(Extent(0, 1)))
        new_ppn = yield env.process(mappings.lpn_to_ppn(lpn=0))

        self.assertNotEqual(old_ppn, new_ppn)

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, victim_block = victim_blocks[0]
        self.assertEqual(oob.states.block_valid_ratio(victim_block), (n-1.0)/n)

        s = env.now
        yield env.process(cleaner.clean())

        # check the time to move n-1 valid pages
        self.assertEqual(env.now,
            s + (n-1)*(time_read_page+time_program_page) + time_erase_block)

        # check validation
        self.assertEqual(oob.states.block_valid_ratio(victim_block), 0)
        ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        for ppn in range(ppn_s, ppn_e):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        self.assertNotIn(victim_block, block_pool.current_blocks())
        self.assertNotIn(victim_block, block_pool.used_blocks)
        self.assertIn(victim_block, block_pool.freeblocks)

        # check mapping
        ppn = yield env.process(mappings.lpn_to_ppn(0))
        block, _ = conf.page_to_block_off(ppn)
        self.assertNotEqual(block, victim_block)


class TestLevelingWear(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_low_threshold_ratio = 0
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        cleaner = dftl.get_cleaner()

        victims = wiscsim.dftldes.WearLevelingVictimBlocks(objs['conf'],
                block_pool, oob, 1)
        datablockcleaner = wiscsim.dftldes.DataBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            rec = objs['rec'],
            env = objs['env'])

        n = conf.n_pages_per_block
        yield env.process(dftl.write_ext(Extent(0, n)))
        old_ppn = yield env.process(mappings.lpn_to_ppn(lpn=0))
        yield env.process(dftl.write_ext(Extent(0, 1)))
        new_ppn = yield env.process(mappings.lpn_to_ppn(lpn=0))

        self.assertNotEqual(old_ppn, new_ppn)

        victim_blocks = list(victims.iterator_verbose())
        self.assertEqual(len(victim_blocks), 1)
        valid_ratio, block_type, victim_block = victim_blocks[0]

        yield env.process(cleaner.level_wear())

        # check validation
        # These tests won't pass because the number of used blocks is too
        # small for the current wear-leveling strategy to work.
        # Our current strategy is to move least erased blocks to most used
        # blocks. But this creates problems because blocks that are erased 0 times
        # will immediately become most used after moving data.
        # ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        # for ppn in range(ppn_s, ppn_e):
            # self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        # self.assertNotIn(victim_block, block_pool.current_blocks())
        # self.assertNotIn(victim_block, block_pool.used_blocks)
        # self.assertIn(victim_block, block_pool.freeblocks)

        # check mapping
        ppn = yield env.process(mappings.lpn_to_ppn(0))
        block, _ = conf.page_to_block_off(ppn)
        self.assertNotEqual(block, victim_block)


class TestCleaningTransBlocksByCleaner(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_low_threshold_ratio = 0
        # let cache just enough to hold one translation page
        conf.n_cache_entries = conf.n_mapping_entries_per_page
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def evict(self, env, mappings, lpn0, lpn1, n_evictions):
        """
        lpn0 and lpn1 are in two different m_vpns
        try to get n_evictions
        """
        yield env.process(mappings.lpn_to_ppn(lpn0))
        yield env.process(mappings.update(lpn=lpn0, ppn=8))

        lpns = [lpn1, lpn0] * n_evictions
        lpns = lpns[:n_evictions]
        for (i, lpn) in enumerate(lpns):
            # every iteration will evict one translation page
            yield env.process(mappings.lpn_to_ppn(lpn))
            yield env.process(mappings.update(lpn=lpn, ppn=i))

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        directory = dftl.get_directory()
        cleaner = dftl.get_cleaner()

        # there should be a current translation block
        self.assertGreater(len(block_pool.current_blocks()), 0)

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)
        transblockcleaner = wiscsim.dftldes.TransBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            directory = objs['directory'],
            rec = objs['rec'],
            env = objs['env'],
            trans_page_locks = objs['trans_page_locks']
            )

        k = conf.n_mapping_entries_per_page
        n = conf.n_pages_per_block
        n_tp = conf.total_translation_pages()

        # try to evict many so we have a used translation block
        yield env.process(self.evict(env, mappings, 0, k, n/2))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, victim_block = victim_blocks[0]

        self.assertEqual(valid_ratio,
                (min(n_tp, conf.n_pages_per_block) - 2.0) / n)

        s = env.now
        yield env.process(cleaner.clean())

        # check the time to move n-1 valid pages
        self.assertEqual(env.now,
                s + (min(n_tp, conf.n_pages_per_block)-2)*\
                        (time_read_page+time_program_page)+time_erase_block)

        # check validation
        self.assertEqual(oob.states.block_valid_ratio(victim_block), 0)
        ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        for ppn in range(ppn_s, ppn_e):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        self.assertNotIn(victim_block, block_pool.current_blocks())
        self.assertNotIn(victim_block, block_pool.used_blocks)
        self.assertIn(victim_block, block_pool.freeblocks)

        # check directory
        self.assertNotEqual(conf.page_to_block_off(directory.m_vpn_to_m_ppn(0))[0],
                victim_block)

@unittest.skipUnless(TESTALL == True, "Skip unless we want to test all")
class TestCleaningTransBlocksByCleaner4Channel(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_low_threshold_ratio = 0
        # let cache just enough to hold one translation page
        conf.n_cache_entries = conf.n_mapping_entries_per_page
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def evict(self, env, mappings, lpn0, lpn1, n_evictions):
        """
        lpn0 and lpn1 are in two different m_vpns
        try to get n_evictions
        """
        yield env.process(mappings.lpn_to_ppn(lpn0))
        yield env.process(mappings.update(lpn=lpn0, ppn=8))

        lpns = [lpn1, lpn0] * n_evictions
        lpns = lpns[:n_evictions]
        for (i, lpn) in enumerate(lpns):
            # every iteration will evict one translation page
            yield env.process(mappings.lpn_to_ppn(lpn))
            yield env.process(mappings.update(lpn=lpn, ppn=i))

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        directory = dftl.get_directory()
        cleaner = dftl.get_cleaner()

        # there should be a current translation block
        self.assertGreater(len(block_pool.current_blocks()), 0)

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)
        transblockcleaner = wiscsim.dftldes.TransBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            directory = objs['directory'],
            rec = objs['rec'],
            env = objs['env'],
            trans_page_locks = objs['trans_page_locks']
            )

        k = conf.n_mapping_entries_per_page
        n = conf.n_pages_per_block
        n_tp = conf.total_translation_pages()

        # try to evict many so we have a used translation block
        yield env.process(self.evict(env, mappings, 0, k, 4*n))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, victim_block = victim_blocks[0]

        init_n_tp_per_block = n_tp / 4
        self.assertEqual(valid_ratio, (init_n_tp_per_block - 1.0) / n)

        s = env.now
        yield env.process(cleaner.clean())

        # check the time to move init_n_tp_per_block valid pages
        # self.assertEqual(env.now,
                # s + init_n_tp_per_block*\
                        # (time_read_page+time_program_page)+time_erase_block)

        # check validation
        for _, _, victim_blocknum in victim_blocks:
            self.assertEqual(oob.states.block_valid_ratio(victim_blocknum), 0)
            ppn_s, ppn_e = conf.block_to_page_range(victim_blocknum)
            for ppn in range(ppn_s, ppn_e):
                self.assertEqual(oob.states.is_page_erased(ppn), True)

            # check blockpool
            self.assertNotIn(victim_blocknum, block_pool.current_blocks())
            self.assertNotIn(victim_blocknum, block_pool.used_blocks)
            self.assertIn(victim_blocknum, block_pool.freeblocks)

            # check directory
            self.assertNotEqual(conf.page_to_block_off(directory.m_vpn_to_m_ppn(0))[0],
                    victim_blocknum)



class TestCleaning(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_low_threshold_ratio = 0
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        cleaner = dftl.get_cleaner()

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)
        datablockcleaner = wiscsim.dftldes.DataBlockCleaner(
            conf = objs['conf'],
            flash = objs['flash_controller'],
            oob = oob,
            block_pool = block_pool,
            mappings = mappings,
            rec = objs['rec'],
            env = objs['env'])

        n_data_used_blocks = len(block_pool.data_usedblocks)

        # create 3 blocks with no valid pages, 1 with full valid pages
        n = conf.n_pages_per_block
        yield env.process(dftl.write_ext(Extent(0, n)))
        yield env.process(dftl.write_ext(Extent(0, n)))
        yield env.process(dftl.write_ext(Extent(0, n)))
        yield env.process(dftl.write_ext(Extent(0, n)))

        victim_blocks = list(victims.iterator_verbose())
        valid_ratio, block_type, victim_block = victim_blocks[0]

        s = env.now
        # yield env.process(datablockcleaner.clean(victim_block))
        yield env.process(cleaner.clean())
        # check the time to move 0 valid pages and erase 3 blocks
        self.assertEqual(env.now, s + 3*time_erase_block)

        # check validation
        self.assertEqual(oob.states.block_valid_ratio(victim_block), 0)
        ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        for ppn in range(ppn_s, ppn_e):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        self.assertNotIn(victim_block, block_pool.current_blocks())
        self.assertNotIn(victim_block, block_pool.used_blocks)
        self.assertIn(victim_block, block_pool.freeblocks)

        # check mapping
        ppn = yield env.process(mappings.lpn_to_ppn(0))
        block, _ = conf.page_to_block_off(ppn)
        self.assertNotEqual(block, victim_block)

class TestCleaning4Channel(unittest.TestCase):
    def test(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        conf.GC_low_threshold_ratio = 0
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlTest(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        conf = objs['conf']
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        time_read_page = objs['flash_controller'].channels[0].read_time
        time_program_page = objs['flash_controller'].channels[0].program_time
        time_erase_block = objs['flash_controller'].channels[0].erase_time

        block_pool = dftl.block_pool
        oob = dftl.oob
        mappings = dftl.get_mappings()
        cleaner = dftl.get_cleaner()

        victims = wiscsim.dftldes.VictimBlocks(objs['conf'], block_pool, oob)

        n_data_used_blocks = len(block_pool.data_usedblocks)

        n = conf.n_pages_per_block
        self.assertEqual(env.now, 0)
        yield env.process(dftl.write_ext(Extent(0, n)))
        # read a translation page and write n pages to the same channel
        self.assertEqual(env.now, time_read_page + time_program_page * n)
        yield env.process(dftl.write_ext(Extent(0, n)))
        # the translation table is in memory, you only need to program pages
        self.assertEqual(env.now, time_read_page + 2 * time_program_page * n)
        yield env.process(dftl.write_ext(Extent(0, n)))
        # the translation table is in memory, you only need to program pages
        self.assertEqual(env.now, time_read_page + 3 * time_program_page * n)
        yield env.process(dftl.write_ext(Extent(0, n)))
        # the translation table is in memory, you only need to program pages
        self.assertEqual(env.now, time_read_page + 4 * time_program_page * n)

        victim_blocks = list(victims.iterator_verbose())
        # you should have 3 blocks because you overwrote the same logical
        # block 3 times
        self.assertEqual(len(victim_blocks), 3)

        valid_ratio, block_type, victim_block = victim_blocks[0]

        s = env.now
        yield env.process(cleaner.clean())
        if cleaner.n_cleaners == 1:
            self.assertEqual(env.now, s + time_erase_block * 3) #<--------
        elif cleaner.n_cleaners >= 4:
            self.assertEqual(env.now, s + time_erase_block)


        # check validation
        self.assertEqual(oob.states.block_valid_ratio(victim_block), 0)
        ppn_s, ppn_e = conf.block_to_page_range(victim_block)
        for ppn in range(ppn_s, ppn_e):
            self.assertEqual(oob.states.is_page_erased(ppn), True)

        # check blockpool
        self.assertNotIn(victim_block, block_pool.current_blocks())
        self.assertNotIn(victim_block, block_pool.used_blocks)
        self.assertIn(victim_block, block_pool.freeblocks)

        # check mapping
        ppn = yield env.process(mappings.lpn_to_ppn(0))
        block, _ = conf.page_to_block_off(ppn)
        self.assertNotEqual(block, victim_block)


class Experiment(object):
    def __init__(self, para):
        self.para = para
    def setup_config(self):
        self.conf = wiscsim.dftldes.Config()
        self.conf['SSDFramework']['ncq_depth'] = 4

        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 1
        self.conf['stripe_size'] = 'infinity'

        self.conf.GC_high_threshold_ratio = 0.001
        self.conf.GC_low_threshold_ratio = 0

        self.conf['do_not_check_gc_setting'] = True


    def setup_environment(self):
        utils.set_exp_metadata(self.conf, save_data = True,
                expname = self.para.expname,
                subexpname = utils.chain_items_as_filename(self.para))

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD

        n_pages_per_block = self.conf.n_pages_per_block

        self.conf["workload_src"] = LBAGENERATOR
        self.conf["lba_workload_class"] = "ExtentTestWorkloadFLEX2"
        self.conf["lba_workload_configs"]["ExtentTestWorkloadFLEX2"] = {
                "events": [
                    (w, 0, n_pages_per_block),
                    (w, 0, n_pages_per_block),
                    (w, 0, n_pages_per_block),
                    (w, 0, n_pages_per_block)
                    ]}
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftldes'
        self.conf['simulator_class'] = 'SimulatorDESNew'

        logicsize_mb = 2
        self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page \
                * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
        self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

    def my_run(self):
        utils.runtime_update(self.conf)
        run_workflow(self.conf)

    def main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestSimpleGC(unittest.TestCase):
    def test(self):
        Parameters = collections.namedtuple("Parameters",
                "expname")
        expname = 'test'

        exp = Experiment( Parameters(expname = expname) )
        exp.main()


class TestSegmenting(unittest.TestCase):
    def test_split(self):
        ext = Extent(0, 9)
        exts = split_ext_by_segment(4, ext)

        self.assertListEqual(list(exts[0].lpn_iter()), [0, 1, 2, 3])
        self.assertListEqual(list(exts[1].lpn_iter()), [4, 5, 6, 7])
        self.assertListEqual(list(exts[2].lpn_iter()), [8])

    def test_split_2(self):
        ext = Extent(5, 1)
        exts = split_ext_by_segment(4, ext)

        self.assertListEqual(list(exts[1].lpn_iter()), [5])

    def test_mapping(self):
        conf = create_config()
        conf['segment_bytes'] = conf.total_flash_bytes()
        conf['stripe_size'] = 1
        objs = create_obj_set(conf)

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        # all lpn in the same segment
        mapping = dftl.get_ppns_to_write(Extent(0, 5))
        channels = set()
        for lpn, ppn in mapping.items():
            channel_id = ppn / conf.n_pages_per_channel
            channels.add(channel_id)
        self.assertEqual(len(channels), 4)

    def test_mapping_with_segment(self):
        conf = create_config()
        conf['segment_bytes'] = 4 * conf.page_size
        conf['stripe_size'] = 1
        objs = create_obj_set(conf)

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        # seg 0
        mapping = dftl.get_ppns_to_write(Extent(1, 1))
        ppn0 = mapping.values()[0]
        channel_id0 = ppn0 / conf.n_pages_per_channel

        # seg 1
        mapping = dftl.get_ppns_to_write(Extent(7, 1))
        mapping = dftl.get_ppns_to_write(Extent(7, 1))
        mapping = dftl.get_ppns_to_write(Extent(7, 1))
        mapping = dftl.get_ppns_to_write(Extent(7, 1))
        ppn1 = mapping.values()[0]
        channel_id1 = ppn1 / conf.n_pages_per_channel
        self.assertEqual(channel_id0, channel_id1)
        # it uses another block
        self.assertEqual(abs(ppn1 - ppn0), conf.n_pages_per_block)


class TestFTLSegmentedWrite(unittest.TestCase):
    def test_valid_ratio(self):
        conf = create_config()
        conf['segment_bytes'] = 2 * conf.page_size
        conf['flash_config']['n_channels_per_dev'] = 4
        conf['stripe_size'] = 'infinity'
        conf.set_flash_num_blocks_by_bytes(128*MB)
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = wiscsim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

        env.process(self.proc_test_write(objs, dftl))
        env.run()

    def proc_test_write(self, objs, dftl):
        env = objs['env']
        rec = objs['rec']
        rec.enable()

        block_pool = dftl.block_pool
        oob = dftl.oob

        conf = objs['conf']
        # should use 5 more blocks
        n_used = block_pool.total_used_blocks()
        yield env.process(dftl.write_ext(Extent(0, 9)))

        n_used2 = block_pool.total_used_blocks()
        self.assertEqual(n_used2 - n_used, 5)


def main():
    unittest.main()

if __name__ == '__main__':
    main()





