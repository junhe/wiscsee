import unittest

import FtlSim


class Test_Event2(unittest.TestCase):
    def test1(self):
        e = FtlSim.simulator.Event(
                512, 0, 'write', 0, 1024)
        print e
        e2 = FtlSim.simulator.Event2(
                512, 0, 'write', 0, 1024, True)
        self.assertEqual(e2.sync, True)

def main():
    unittest.main()

if __name__ == '__main__':
    main()






