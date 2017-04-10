import unittest

import wiscsim
from utilities import utils
from wiscsim.bitmap import FlashBitmap2

def create_config():
    conf = wiscsim.dftldes.Config()
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

def create_bitmap(conf):
    bitmap = FlashBitmap2(conf)
    return bitmap


class TestBitmap(unittest.TestCase):
    def test_create(self):
        conf = create_config()
        bitmap = create_bitmap(conf)

    def test_init_states(self):
        conf = create_config()
        bitmap = create_bitmap(conf)

        for ppn in range(conf.total_num_pages()):
            self.assertEqual(bitmap.is_page_valid(ppn), False)
            self.assertEqual(bitmap.is_page_invalid(ppn), False)
            self.assertEqual(bitmap.is_page_erased(ppn), True)

    def test_validating(self):
        conf = create_config()
        bitmap = create_bitmap(conf)

        self.assertEqual(bitmap.block_valid_ratio(0), 0)
        self.assertEqual(bitmap.block_invalid_ratio(0), 1)
        bitmap.validate_block(0)
        self.assertEqual(bitmap.block_valid_ratio(0), 1)
        self.assertEqual(bitmap.block_invalid_ratio(0), 0)

    def test_invalidating(self):
        conf = create_config()
        bitmap = create_bitmap(conf)

        bitmap.validate_block(0)
        bitmap.invalidate_page(0)
        self.assertEqual(bitmap.is_page_invalid(0), True)
        self.assertEqual(bitmap.page_state(0), bitmap.INVALID)
        self.assertEqual(bitmap.block_valid_ratio(0),
                1 - 1.0/conf.n_pages_per_block)


def main():
    unittest.main()

if __name__ == '__main__':
    main()




