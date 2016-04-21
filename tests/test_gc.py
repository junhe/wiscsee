import unittest

import ssdbox
from utilities import utils

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

    conf['ftl_type'] = 'dftldes'
    conf['simulator_class'] = 'SimulatorDESSync'

    devsize_mb = 64
    conf.n_cache_entries = conf.n_mapping_entries_per_page
    conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    utils.runtime_update(conf)

    return conf


def create_blockpool(conf):
    return ssdbox.dftldes.BlockPool(conf)


def create_oob(conf):
    oob = ssdbox.dftldes.OutOfBandAreas(conf)
    return oob

def create_vbs():
    conf = create_config()
    block_pool = create_blockpool(conf)
    oob = create_oob(conf)

    vbs = ssdbox.dftldes.VictimBlocks(conf, block_pool, oob)

    return vbs


class TestVictimBlocks(unittest.TestCase):
    def test_entry(self):
        ssdbox.dftldes.VictimBlocks

    def test_init(self):
        create_vbs()

    def test_empty(self):
        vbs = create_vbs()
        self.assertEqual(len(list(vbs.iterator())), 0)

    def test_cur_blocks(self):
        """
        cur blocks should not be victim
        """
        conf = create_config()
        block_pool = create_blockpool(conf)
        oob = create_oob(conf)

        vbs = ssdbox.dftldes.VictimBlocks(conf, block_pool, oob)

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

        vbs = ssdbox.dftldes.VictimBlocks(conf, block_pool, oob)

        victims = list(vbs.iterator())
        self.assertEqual(victims, [0])

    def test_3_victim_candidates(self):
        conf = create_config()
        conf['flash_config']['n_channels_per_dev'] = 1
        conf['stripe_size'] = 'infinity'
        block_pool = create_blockpool(conf)
        oob = create_oob(conf)

        # use n-2 pages
        n = conf.n_pages_per_block
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        oob.invalidate_ppns(ppns[:-2])

        # use n-1 pages
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        block1, _ = conf.page_to_block_off(ppns[0])
        oob.invalidate_ppns(ppns[:-1])

        # use n-3 pages
        ppns = block_pool.next_n_data_pages_to_program_striped(n)
        oob.invalidate_ppns(ppns[:-3])

        # use one more
        ppns = block_pool.next_n_data_pages_to_program_striped(1)

        vbs = ssdbox.dftldes.VictimBlocks(conf, block_pool, oob)

        victims = list(vbs.iterator())

        self.assertEqual(victims[0], block1)













def main():
    unittest.main()

if __name__ == '__main__':
    main()




