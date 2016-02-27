import unittest
import WlRunner

class TestCpuhandler(unittest.TestCase):
    def test_cpu(self):
        possible_cpus = WlRunner.cpuhandler.get_possible_cpus()
        WlRunner.cpuhandler.enable_all_cpus()

        online_cpus = WlRunner.cpuhandler.get_online_cpuids()
        self.assertListEqual(possible_cpus, online_cpus)

def main():
    unittest.main()

if __name__ == '__main__':
    main()


