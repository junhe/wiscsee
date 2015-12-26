import simpy
from commons import *


class OutOfBandArea(object):
    def __init__(self, env, conf):
        self.env = env
        self.state = PAGE_ERASED #PAGE_PROGRAMMED, PAGE_ERASED
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
        return self.value

    def __write_latency_proc(self, value):
        yield self.env.timeout(self.write_latency)
        self.value = value

    def read(self):
        """
        We allows reading an unwritten page. So we don't check the
        state of the page here.
        """
        ret = yield self.env.process(self.__read_latency_proc())
        return ret

    def write(self, value):
        assert self.get_state() == PAGE_ERASED
        yield self.env.process(self.__write_latency_proc(value))
        self.set_state(PAGE_PROGRAMMED)

    def set_state(self, state):
        self.ooba.state = state

    def get_state(self):
        return self.ooba.state


class Block(object):
    def __init__(self, env, conf):
        self.env = env
        self.conf = conf
        self.npages = self.conf['n_pages_per_block']
        self.erase_time = self.conf['block_erase_time']

        self.pages = [ Page(env, conf) for i in range(self.npages) ]

    def get_page(self, page_offset):
        assert page_offset < self.npages
        return self.pages[page_offset]

    def read_page(self, page_offset):
        """
        the first page in the block has page_offset = 0
        """
        page = self.get_page(page_offset)
        ret = yield self.env.process( page.read() )
        return ret

    def write_page(self, page_offset, value):
        page = self.get_page(page_offset)
        ret = yield self.env.process( page.write(value) )
        return ret

    def erase_block(self):
        yield self.env.timeout( self.erase_time )
        for page in self.pages:
            page.set_state(PAGE_ERASED)



if __name__ == '__main__':
    main()





