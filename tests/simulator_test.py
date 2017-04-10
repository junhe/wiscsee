import unittest
import wiscsim
from wiscsim.ftlsim_commons import *


class TestNCQSingleQueue(unittest.TestCase):
    def test_holding_slots(self):
        env = simpy.Environment()
        ncq = NCQSingleQueue(2, env)

        env.process(self.process(env, ncq))
        env.run()

    def process(self, env, ncq):
        held_slot_reqs = yield env.process(ncq.hold_all_slots())
        ncq.release_all_slots(held_slot_reqs)


class TestNCQSingleQueueWithWaitTime(unittest.TestCase):
    def test_holding_slots(self):
        env = simpy.Environment()
        ncq = NCQSingleQueue(2, env)

        env.process(self.main_proc(env, ncq))
        env.run()

    def main_proc(self, env, ncq):
        env.process(self.use_one_slot(env, ncq))
        yield env.process(self.wait_all(env, ncq))

        self.assertEqual(env.now, 5)

    def wait_all(self, env, ncq):
        held_slot_reqs = yield env.process(ncq.hold_all_slots())
        self.assertEqual(env.now, 5)
        ncq.release_all_slots(held_slot_reqs)

    def use_one_slot(self, env, ncq):
        req = ncq.slots.request()
        yield req

        yield env.timeout(5)

        ncq.slots.release(req)


def main():
    unittest.main()

if __name__ == '__main__':
    main()






