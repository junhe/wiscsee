import simpy

SIM_DURATION = 20

ERASED, VALID, INVALID = 'ERASED', 'VALID', 'INVALID'

class Page(object):
    """
    This Page class does not enforce the queuing of read/write
    requests because in real hardware there is no queue. I guess
    in real hardware, the requests are serialized at the instruction
    bus, and if two requests to the same page are close in time,
    the second one should return failure (instead be-ing queued).

    TODO: check how the hardware handle race of the same page.
    """
    def __init__(self, env, read_latency, write_latency):
        self.env = env
        self.state = ERASED
        self.value = None

        self.read_latency = read_latency
        self.write_latency = write_latency

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
        self.state = state

def accessor_proc(env, page):
    """
    write and read page in serial
    """
    cnt = 0
    while True:
        print 'try to write at', env.now
        yield env.process(page.write(cnt))
        print 'try to read at', env.now
        ret = yield env.process(page.read())
        print 'read', ret

        cnt += 1

def main():
    print('Page Latency')
    env = simpy.Environment()

    page = Page(env, read_latency = 1, write_latency = 3)
    env.process(accessor_proc(env, page))

    env.run(until=SIM_DURATION)

if __name__ == '__main__':
    main()

