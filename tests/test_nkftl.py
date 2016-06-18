import unittest
import random

from ssdbox.nkftl2 import Nkftl
import ssdbox
import config
from commons import *
from utilities import utils

def create_config():
    conf = ssdbox.nkftl2.Config()

    conf['flash_config']['n_pages_per_block'] = 64
    conf['flash_config']['n_blocks_per_plane'] = 2
    conf['flash_config']['n_planes_per_chip'] = 1
    conf['flash_config']['n_chips_per_package'] = 1
    conf['flash_config']['n_packages_per_channel'] = 1
    conf['flash_config']['n_channels_per_dev'] = 4

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

class TestNkftl(unittest.TestCase):
    def test_init(self):
        ftl, conf, rec = create_nkftl()

    def test_write_and_read(self):
        ftl, conf, rec = create_nkftl()

        ftl.lba_write(8, data='3')
        ret = ftl.lba_read(8)
        self.assertEqual(ret, '3')

    def test_data_integrity(self):
        ftl, conf, rec = create_nkftl()

        total_pages = conf.total_num_pages()
        lpns = random.sample(range(total_pages), 1000)

        data_dict = {lpn:str(lpn) for lpn in lpns}
        for lpn, data in data_dict.items():
            ftl.lba_write(lpn, data)

        for lpn, data in data_dict.items():
            ret = ftl.lba_read(lpn)
            print ret
            self.assertEqual(ret, data)

def main():
    unittest.main()

if __name__ == '__main__':
    main()


