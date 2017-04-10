import socket
import unittest
import time
import copy
import pprint

import workrunner
import wiscsim
from utilities import utils
from config import MountOption as MOpt
from config import ConfigNCQFTL
from workflow import run_workflow
from wiscsim.simulator import GcLog
from wiscsim.ftlsim_commons import Extent, random_channel_id
from wiscsim.ftlcounter import LpnClassification, get_file_range_table, EventNCQParser
from wiscsim import hostevent
from config_helper.rule_parameter import EventFileSets
from commons import *

class TestCpuhandler(unittest.TestCase):
    def test_cpu(self):
        possible_cpus = workrunner.cpuhandler.get_possible_cpus()
        workrunner.cpuhandler.enable_all_cpus()

        online_cpus = workrunner.cpuhandler.get_online_cpuids()
        self.assertListEqual(possible_cpus, online_cpus)

class TestRandomChannelID(unittest.TestCase):
    def test(self):
        n = 16

        channels = set()
        for i in range(10000):
            channel_id = random_channel_id(n)
            channels.add(channel_id)
            self.assertTrue(channel_id >= 0)
            self.assertTrue(channel_id < n)

        self.assertTrue(len(channels) > n/3)

@unittest.skip("Need real device that supports NCQ")
class TestLinuxNCQDepth(unittest.TestCase):
    def test_ncq_depth_setting(self):
        if not 'wisc.cloudlab.us' in socket.gethostname():
            return

        depth = 2
        utils.set_linux_ncq_depth("sdc", depth)
        read_depth = utils.get_linux_ncq_depth("sdc")
        self.assertEqual(depth, read_depth)

@unittest.skip("Need real device that supports setting scheduler")
class TestSettingScheduler(unittest.TestCase):
    def test_setting(self):
        scheduler = 'noop'
        utils.set_linux_io_scheduler("sdc", scheduler)

        read_scheduler = utils.get_linux_io_scheduler("sdc")
        self.assertEqual(scheduler, read_scheduler)


class Experiment(object):
    def __init__(self):
        self.conf = wiscsim.dftldes.Config()

    def setup_environment(self):
        self.conf['device_path'] = '/dev/loop0'
        self.conf['dev_size_mb'] = 256
        self.conf['filesystem'] = 'f2fs'
        self.conf["n_online_cpus"] = 'all'

        self.conf['linux_ncq_depth'] = 31
        self.conf['sort_block_trace'] = True

    def setup_workload(self):
        self.conf['workload_class'] = 'NoOp'
        self.conf['NoOp'] = {}
        self.conf['workload_conf_key'] = 'NoOp'

    def setup_fs(self):
        pass
        # self.conf['mnt_opts'].update({
            # "f2fs":   {
                        # 'discard': MOpt(opt_name = 'discard',
                                        # value = 'discard',
                                        # include_name = False),
                        # 'background_gc': MOpt(opt_name = 'background_gc',
                                            # value = 'off',
                                            # include_name = True)
                                        # }
            # }
            # )

    def setup_flash(self):
        pass

    def setup_ftl(self):
        self.conf['enable_blktrace'] = False
        self.conf['enable_simulation'] = False

    def run(self):
        utils.set_exp_metadata(self.conf, save_data = False,
                expname = 'tmp',
                subexpname = 'subtmp')
        utils.runtime_update(self.conf)
        run_workflow(self.conf)

        utils.shcmd("fio -name hello -rw=randwrite -size=16mb -fsync=1  -filename {}/data2"\
                .format(self.conf['fs_mount_point']))
        time.sleep(1)
        ret = utils.invoke_f2fs_gc(self.conf['fs_mount_point'], 1)
        assert ret == 0

    def main(self):
        self.setup_environment()
        self.setup_fs()
        self.setup_workload()
        self.setup_flash()
        self.setup_ftl()
        self.run()

@unittest.skip("Need FIO to create some random workload to create F2FS garbage")
class TestF2FSGCCall(unittest.TestCase):
    def test(self):
        obj = Experiment()
        obj.main()

class TestImportPyreuse(unittest.TestCase):
    def test(self):
        import pyreuse
        pyreuse.helpers.shcmd("echo 33333")

class TestClassifyGcLOG(unittest.TestCase):
    @unittest.skip("Need real device mounted")
    def test(self):
        gclog = GcLog(
                device_path='/dev/sdc1',
                result_dir='/tmp/results/1gbnojournalok/Leveldb.ext4.1gbnojournalok.4294967296.devsdc1.128.31.1073741824.1073741824.True.ordered.False.overwrite.1000000.1.True.64.4.4.2000000.dftldes.1-ext4-06-29-08-06-52--7574556694461561217',
                flash_page_size=2048
                )
        print gclog._get_range_table()
        gclog.classify_lpn_in_gclog()

class TestExtent(unittest.TestCase):
    def test_copy(self):
        ext1 = Extent(lpn_start=3, lpn_count=8)
        ext2 = copy.copy(ext1)

        self.assertEqual(ext1.lpn_start, ext2.lpn_start)
        self.assertEqual(ext1.lpn_count, ext2.lpn_count)
        self.assertNotEqual(ext1, ext2)

        ext2.lpn_start = 100
        self.assertEqual(ext1.lpn_start, 3)


class TestGroupToBatches(unittest.TestCase):
    def test(self):
        a = [0, 1, 2]
        batches = utils.group_to_batches(a, 1)
        self.assertListEqual(batches, [[0], [1], [2]])

    def test_larger(self):
        a = range(7)
        batches = utils.group_to_batches(a, 3)
        self.assertListEqual(batches, [[0, 1, 2],
                                       [3, 4, 5],
                                       [6]])

@unittest.skip('need real device')
class TestLpnClassification(unittest.TestCase):
    def test(self):
        classifier = LpnClassification(
                lpns = [1, 8],
                device_path = '/dev/sdc1',
                result_dir = '/tmp/results/test002/subexp--6155052293192590053-ext4-09-16-09-32-22-1439596482389025085',
                flash_page_size = 2048)
        classifier.classify()


class TestNCQParser(unittest.TestCase):
    def test(self):
        # blkparse-events-for-ftlsim.txt
        conf = ConfigNCQFTL()

        workload_line_iter = hostevent.FileLineIterator(
            "tests/testdata/blkparse-events-for-ftlsim.txt")
        event_workload_iter = hostevent.EventIterator(conf, workload_line_iter)

        # for event in event_workload_iter:
            # print str(event)

        parser = EventNCQParser(event_workload_iter)
        table = parser.parse()

        self.assertEqual(table[0]['pre_depth'], 0)
        self.assertEqual(table[0]['post_depth'], 1)

        self.assertEqual(table[1]['pre_depth'], 1)
        self.assertEqual(table[1]['post_depth'], 0)


class TestEventFileSets(unittest.TestCase):
    def test(self):
        filesets = EventFileSets('tests/testdata/64mbfile')
        sets = filesets.get_sets()
        self.assertEqual(len(sets), 1)
        self.assertEqual(sets[0]['mkfs_path'],
            'tests/testdata/64mbfile/subexp-3563455040949707047-ext4-10-05-16-29-19-3141981191822244772/blkparse-events-for-ftlsim-mkfs.txt')



def main():
    unittest.main()

if __name__ == '__main__':
    main()


