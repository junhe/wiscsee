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
    entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_config']['page_size'])
    print 'entries_need', entries_need
    conf.max_cmt_bytes = int(entries_need * 8) # 8 bytes (64bits) needed in mem
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

def create_obj_set():
    conf = create_config()
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


class TestMappingCache(unittest.TestCase):
    def writing_proc(self, env, mapping_cache, flash, conf, objs):
        return
        mapping_dict = FtlSim.dftldes.MappingDict()
        for lpn in range(conf.n_mapping_entries_per_page):
            mapping_dict[lpn] = lpn * 100
        self.assertEqual(len(mapping_dict), conf.n_mapping_entries_per_page)

        yield env.process(mapping_cache.update_mapping_on_flash(0,
            mapping_dict))

        self.assertEqual(flash.channels[0].program_time, env.now)
        self.assertEqual(100, objs['mapping_on_flash'].lpn_to_ppn(1))
        self.assertNotEquals(objs['directory'].m_vpn_to_m_ppn(0), 0)


    def test_writing(self):
        objs = create_obj_set()
        mapping_cache = create_mapping_cache(objs)
        env = objs['env']

        env.process(self.writing_proc(env, mapping_cache,
            objs['flashctrler'], objs['conf'], objs))
        env.run()

    def loading_proc(self, env, mapping_cache, flash, conf):
        return
        yield env.process(
                mapping_cache.load(0))
        self.assertEqual(flash.channels[0].read_time, env.now)

    def test_loading(self):
        objs = create_obj_set()
        mapping_cache = create_mapping_cache(objs)
        env = objs['env']

        env.process(self.loading_proc(env, mapping_cache,
            objs['flashctrler'], objs['conf']))
        env.run()

    def evict_proc(self, env, mapping_cache, flash, conf, objs):
        return
        mapping_on_flash = objs['mapping_on_flash']

        ppn = yield env.process( mapping_cache.lpn_to_ppn(0) )
        self.assertEqual(ppn, FtlSim.dftldes.UNINITIATED)

        mapping_cache.update(lpn = 0, ppn = 1000)
        ppn = yield env.process( mapping_cache.lpn_to_ppn(0) )
        self.assertEqual(ppn, 1000)

        self.assertEqual(mapping_on_flash.lpn_to_ppn(0),
            FtlSim.dftldes.UNINITIATED)
        yield env.process( mapping_cache.evict(m_vpn = 0) )
        self.assertEqual(mapping_on_flash.lpn_to_ppn(0), 1000)

    def test_evict(self):
        objs = create_obj_set()
        mapping_cache = create_mapping_cache(objs)
        env = objs['env']

        env.process(self.evict_proc(env, mapping_cache,
            objs['flashctrler'], objs['conf'], objs))
        env.run()


def main():
    unittest.main()

if __name__ == '__main__':
    main()





