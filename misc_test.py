import unittest
import WlRunner
import utils

class TestCpuhandler(unittest.TestCase):
    def test_cpu(self):
        possible_cpus = WlRunner.cpuhandler.get_possible_cpus()
        WlRunner.cpuhandler.enable_all_cpus()

        online_cpus = WlRunner.cpuhandler.get_online_cpuids()
        self.assertListEqual(possible_cpus, online_cpus)

class TestLinuxNCQDepth(unittest.TestCase):
    def test_ncq_depth_setting(self):
        depth = 2
        utils.set_linux_ncq_depth("sdc", depth)
        read_depth = utils.get_linux_ncq_depth("sdc")
        self.assertEqual(depth, read_depth)

def main():
    unittest.main()

if __name__ == '__main__':
    main()


