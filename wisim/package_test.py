import unittest
import package

import simpy

class ReadWriteTester(object):
    def __init__(self):
        self.values = []
        self.ret = None

    def accessor_proc(self, env, page):
        """
        write and read page in serial
        """
        rets = []
        for i in [3, 4, 8]:
            yield env.process(page.write(i))
            ret = yield env.process(page.read())
            rets.append(ret)

        self.ret = rets

    def equal(self):
        return cmp(self.values, self.ret)


class PageTest(unittest.TestCase):
    def test_write_and_read(self):
        env = simpy.Environment()

        page = package.Page(env, read_latency = 1, write_latency = 3)

        rwtester = ReadWriteTester()
        env.process(rwtester.accessor_proc(env, page))
        env.run()
        self.assertTrue( rwtester.equal() )

if __name__ == '__main__':
    unittest.main()
