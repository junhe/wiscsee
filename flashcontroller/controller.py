import simpy


class FlashAddress(object):
    def __init__(self):
        self.page = None
        self.block = None
        self.plane = None
        self.chip = None
        self.package = None
        self.channel = None


class FlashRequest(object):
    OP_READ, OP_WRITE, OP_ERASE = 'OP_READ', 'OP_WRITE', 'OP_ERASE'
    def __init__(self):
        self.addr = None
        self.operation = None


class Controller(object):
    def __init__(self, simpy_env, conf):
        self.env = simpy_env
        self.conf = conf

        self.page_size = self.conf['flash_config']['page_size']
        self.n_pages_per_block = self.conf['flash_config']['n_pages_per_block']
        self.n_blocks_per_plane = self.conf['flash_config']['n_blocks_per_plane']
        self.n_planes_per_chip = self.conf['flash_config']['n_planes_per_chip']
        self.n_chips_per_package = self.conf['flash_config']['n_chips_per_package']
        self.n_packages_per_channel = self.conf['flash_config']['n_packages_per_channel']
        self.n_channels_per_dev = self.conf['flash_config']['n_channels_per_dev']

        self.channels = [Channel(self.env, conf)
                for _ in range( self.n_channels_per_dev)]

    def write_page(self, addr, data = None):
        """
        Usage:
            if you do:
            yield env.process(controller.write_page(addr))
            the calling process will wait until write_page() finishes

            if you do
            controller.write_page(addr)
            the calling process will not wait

            if you do
            controller.write_page(addr)
            controller.write_page(addr) # to same channel
            the calling process will not wait
            the second write has to wait for the first
        """
        yield self.env.process(
            self.channels[addr.channel].write_page(None))

    def read_page(self, addr):
        yield self.env.process(
            self.channels[addr.channel].read_page(None))

    def erase_block(self, addr):
        yield self.env.process(
            self.channels[addr.channel].erase_block(None))


class Channel(object):
    """
    This is a channel with only single package, chip, and plane. This is how a
    request is processed in it:

    This is a channel without pipelining. It is simply a resource that cannot
    be shared. It simply adds delay to the operation.

    Read:
        7*t_wc + t_R + nbytes*t_rc
    write:
        7*t_wc + nbytes*t_wc + t_prog
    """
    def __init__(self, simpy_env, conf):
        self.env = simpy_env
        self.conf = conf
        self.resource = simpy.Resource(self.env, capacity = 1)

        t_wc = 1
        t_r = 1
        t_rc = 1
        t_prog = 1
        t_erase = 1
        page_size = self.conf['flash_config']['page_size']

        self.read_time = 7 * t_wc + t_r + page_size * t_rc
        self.program_time = 7 * t_wc + page_size * t_wc + t_prog
        self.erase_time = 5 * t_wc + t_erase

    def write_page(self, addr = None , data = None):
        """
        If you want to when this operation is finished, just print env.now.
        If you want to know how long it takes, use env.now before and after
        the operation.
        """
        with self.resource.request() as request:
            yield self.env.timeout( self.program_time )

    def read_page(self, addr = None):
        with self.resource.request() as request:
            yield self.env.timeout( self.read_time )

    def erase_block(self, addr = None):
        with self.resource.request() as request:
            yield self.env.timeout( self.erase_time )



