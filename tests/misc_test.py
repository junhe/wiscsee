import unittest
import workrunner
import socket
from utilities import utils

class TestCpuhandler(unittest.TestCase):
    def test_cpu(self):
        possible_cpus = workrunner.cpuhandler.get_possible_cpus()
        workrunner.cpuhandler.enable_all_cpus()

        online_cpus = workrunner.cpuhandler.get_online_cpuids()
        self.assertListEqual(possible_cpus, online_cpus)

class TestLinuxNCQDepth(unittest.TestCase):
    def test_ncq_depth_setting(self):
        if not 'wisc.cloudlab.us' in socket.gethostname():
            return

        depth = 2
        utils.set_linux_ncq_depth("sdc", depth)
        read_depth = utils.get_linux_ncq_depth("sdc")
        self.assertEqual(depth, read_depth)

class TestSettingScheduler(unittest.TestCase):
    def test_setting(self):
        if not 'wisc.cloudlab.us' in socket.gethostname():
            return

        scheduler = 'noop'
        utils.set_linux_io_scheduler("sdc", scheduler)

        read_scheduler = utils.get_linux_io_scheduler("sdc")
        self.assertEqual(scheduler, read_scheduler)


def main():
    unittest.main()

if __name__ == '__main__':
    main()


