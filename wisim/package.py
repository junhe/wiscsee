import simpy
from commons import *

class OutOfBandArea(object):
    def __init__(self, env, conf):
        self.env = env
        self.state = None #erased, valid, or invalid
        self.conf = conf

class Page(object):
    """
    This Page class does not enforce the queuing of read/write
    requests because in real hardware there is no queue. I guess
    in real hardware, the requests are serialized at the instruction
    bus, and if two requests to the same page are close in time,
    the second one should return failure (instead be-ing queued).

    TODO: check how the hardware handle race of the same page.

    To read a page:
        ret = yield env.process(page.read())
    To write a page:
        yield env.process(page.write(datavalue))
    """
    def __init__(self, env, conf):
        self.env = env
        self.conf = conf
        self.value = None
        self.ooba = OutOfBandArea(env, conf)

        self.read_latency = self.conf['page_read_time']
        self.write_latency = self.conf['page_prog_time']

    def __read_latency_proc(self):
        yield self.env.timeout(self.read_latency)
        self.env.exit(self.value)

    def __write_latency_proc(self, value):
        yield self.env.timeout(self.write_latency)
        self.value = value

    def read(self):
        ret = yield self.env.process(self.__read_latency_proc())
        self.env.exit(ret)

    def write(self, value):
        yield self.env.process(self.__write_latency_proc(value))

    def set_state(self, state):
        self.ooba.state = state

    def get_state(self):
        return self.ooba.state


class Block(object):
    def __init__(self, env, conf):
        self.env = env
        self.conf = conf
        self.npages = self.conf['n_pages_per_block']

        # self.pages = [Page(env


if __name__ == '__main__':
    main()

