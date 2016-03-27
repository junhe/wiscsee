import unittest
import simpy
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
    flashctrler = create_flashcontrolelr(conf, env, rec)
    directory = create_translation_directory(conf, oob, block_pool)
    gmt = create_mapping_on_flash(conf)

    return {'conf':conf, 'rec':rec, 'oob':oob, 'block_pool':block_pool,
            'flashctrler':flashctrler, 'env':env, 'directory':directory,
            'mapping_on_flash':gmt}

def create_mapping_cache(objs):
    mapping_cache = FtlSim.dftldes.MappingCache(
            confobj = objs['conf'],
            block_pool = objs['block_pool'],
            flashobj = objs['flashctrler'],
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
    def update_proc(self, objs, mapping_cache):
        conf = objs['conf']
        env = objs['env']

        m_vpn = 3
        lpns  = conf.m_vpn_to_lpns(m_vpn)
        for lpn in lpns:
            ppn = lpn * 1000
            yield env.process(mapping_cache.update(lpn, ppn))
            ppn_in_cache = yield env.process(mapping_cache.lpn_to_ppn(lpn))
            self.assertEqual(ppn, ppn_in_cache)

        # it should not take any time
        self.assertEqual(env.now, 0)

        m_vpn = 4
        lpns  = conf.m_vpn_to_lpns(m_vpn)
        for lpn in lpns:
            ppn = lpn * 1000
            yield env.process(mapping_cache.update(lpn, ppn))
            ppn_in_cache = yield env.process(mapping_cache.lpn_to_ppn(lpn))
            self.assertEqual(ppn, ppn_in_cache)

    def test_update(self):
        conf = create_config()
        conf.n_cache_entries = conf.n_mapping_entries_per_page
        objs = create_obj_set(conf)

        mapping_cache = create_mapping_cache(objs)

        env = objs['env']
        env.process(self.update_proc(objs, mapping_cache))
        env.run()

def main():
    unittest.main()

if __name__ == '__main__':
    main()





