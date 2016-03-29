import unittest
import simpy

from FtlSim import ftlsim_commons

import FtlSim
import utils
import flashcontroller

def create_config():
    conf = FtlSim.dftldes.Config()
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

    conf['ftl_type'] = 'dftldes'
    conf['simulator_class'] = 'SimulatorDESSync'

    devsize_mb = 64
    conf.n_cache_entries = conf.n_mapping_entries_per_page
    conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


def create_recorder(conf):
    rec = FtlSim.recorder.Recorder(output_target = conf['output_target'],
        output_directory = conf['result_dir'],
        verbose_level = conf['verbose_level'],
        print_when_finished = conf['print_when_finished']
        )
    rec.disable()
    return rec

def create_oob(conf):
    oob = FtlSim.dftldes.OutOfBandAreas(conf)
    return oob

def create_blockpool(conf):
    return FtlSim.dftldes.BlockPool(conf)

def create_flashcontrolelr(conf, env, rec):
    return flashcontroller.controller.Controller3(env, conf, rec)

def create_simpy_env():
    return simpy.Environment()

def create_translation_directory(conf, oob, block_pool):
    return FtlSim.dftldes.GlobalTranslationDirectory(conf, oob, block_pool)

def create_mapping_on_flash(conf):
    return FtlSim.dftldes.MappingOnFlash(conf)

def create_obj_set(conf):
    rec = create_recorder(conf)
    oob = create_oob(conf)
    block_pool = create_blockpool(conf)
    env = create_simpy_env()
    flash_controller = create_flashcontrolelr(conf, env, rec)
    directory = create_translation_directory(conf, oob, block_pool)
    gmt = create_mapping_on_flash(conf)

    return {'conf':conf, 'rec':rec, 'oob':oob, 'block_pool':block_pool,
            'flash_controller':flash_controller, 'env':env, 'directory':directory,
            'mapping_on_flash':gmt}

def create_mapping_cache(objs):
    mapping_cache = FtlSim.dftldes.MappingCache(
            confobj = objs['conf'],
            block_pool = objs['block_pool'],
            flashobj = objs['flash_controller'],
            oobobj = objs['oob'],
            recorderobj = objs['rec'],
            envobj = objs['env'],
            directory = objs['directory'],
            mapping_on_flash = objs['mapping_on_flash'])

    return mapping_cache

class TestMappingTable(unittest.TestCase):
    def test_modification(self):
        config = create_config()
        table = FtlSim.dftldes.MappingTable(config)

        ppn = table.lpn_to_ppn(100)
        self.assertEqual(ppn, FtlSim.dftldes.MISS)

        table.add_new_entry(lpn = 100, ppn = 200, dirty = False)
        ppn = table.lpn_to_ppn(100)
        self.assertEqual(ppn, 200)

        table.overwrite_entry(lpn = 100, ppn = 201, dirty = False)
        ppn = table.lpn_to_ppn(100)
        self.assertEqual(ppn, 201)

    def test_deleting_mvpn(self):
        config = create_config()
        table = FtlSim.dftldes.MappingTable(config)

        table.add_new_entry(lpn = 0, ppn = 100, dirty = False)
        # add an entry of another m_vpn
        table.add_new_entry(lpn = 100000, ppn = 2, dirty = False)

        table.delete_entries_of_m_vpn(m_vpn = 0)
        self.assertEqual(table.lpn_to_ppn(0), FtlSim.dftldes.MISS)
        self.assertEqual(table.lpn_to_ppn(100000), 2)

    def test_size(self):
        config = create_config()
        table = FtlSim.dftldes.MappingTable(config)

        self.assertEqual(config.n_cache_entries, table.max_n_entries)
        self.assertEqual(table.count(), 0)
        table.add_new_entry(lpn = 0, ppn = 100, dirty = False)
        self.assertEqual(table.count(), 1)
        table.remove_entry_by_lpn(lpn = 0)
        self.assertEqual(table.count(), 0)

    def test_lru(self):
        config = create_config()
        table = FtlSim.dftldes.MappingTable(config)

        table.add_new_entry(lpn = 0, ppn = 100, dirty = False)
        table.add_new_entry(lpn = 100000, ppn = 2, dirty = False)

        victim_lpn, _ = table.victim_entry()
        self.assertEqual(victim_lpn, 0)

        _ = table.lpn_to_ppn(0)
        victim_lpn, _ = table.victim_entry()
        self.assertEqual(victim_lpn, 100000)

    def test_quiet_overwrite(self):
        config = create_config()
        table = FtlSim.dftldes.MappingTable(config)

        table.add_new_entry(lpn = 0, ppn = 100, dirty = False)
        table.add_new_entry(lpn = 100000, ppn = 2, dirty = False)

        victim_lpn, _ = table.victim_entry()
        self.assertEqual(victim_lpn, 0)

        table.overwrite_quietly(lpn = 0, ppn = 200, dirty = True)

        victim_lpn, _ = table.victim_entry()
        self.assertEqual(victim_lpn, 0)

        ppn = table.lpn_to_ppn(0)
        self.assertEqual(ppn, 200)


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

        dftl = FtlSim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])


    def test_write(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlSim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

    def proc_test_write(self, env, dftl):
        pass

    def test_read(self):
        conf = create_config()
        objs = create_obj_set(conf)
        env = objs['env']

        dftl = FtlSim.dftldes.Ftl(objs['conf'], objs['rec'],
                objs['flash_controller'], objs['env'])

    def proc_test_read(self, env, dftl):
        pass


class TestSplit(unittest.TestCase):
    def test(self):
        conf = create_config()
        n = conf.n_mapping_entries_per_page

        result = FtlSim.dftldes.split_ext_to_mvpngroups(conf,
                ftlsim_commons.Extent(0, n))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].lpn_start, 0)
        self.assertEqual(result[0].end_lpn(), n)

        result = FtlSim.dftldes.split_ext_to_mvpngroups(conf,
                ftlsim_commons.Extent(0, n + 1))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].lpn_start, 0)
        self.assertEqual(result[0].end_lpn(), n)
        self.assertEqual(result[1].lpn_start, n)
        self.assertEqual(result[1].end_lpn(), n + 1)

def main():
    unittest.main()

if __name__ == '__main__':
    main()





