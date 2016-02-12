import simpy


class FlashAddress(object):
    def __init__(self):
        self.page_index = 5
        self.block_index = 4
        self.plane_index = 3
        self.chip_index = 2
        self.package_index = 1
        self.channel_index = 0

        self.names = ['channel', 'package', 'chip', 'plane', 'block', 'page']
        self.location = [0 for _ in self.names]

    def __str__(self):
        lines = []
        for name, no in zip(self.names, self.location):
            lines.append(name.ljust(8) + str(no))
        return '\n'.join(lines)

    @property
    def page(self):
        return self.location[self.page_index]
    @page.setter
    def page(self, value):
        self.location[self.page_index] = value

    @property
    def block(self):
        return self.location[self.block_index]
    @block.setter
    def block(self, value):
        self.location[self.block_index] = value

    @property
    def plane(self):
        return self.location[self.plane_index]
    @plane.setter
    def plane(self, value):
        self.location[self.plane_index] = value

    @property
    def chip(self):
        return self.location[self.chip_index]
    @chip.setter
    def chip(self, value):
        self.location[self.chip_index] = value

    @property
    def package(self):
        return self.location[self.package_index]
    @package.setter
    def package(self, value):
        self.location[self.package_index] = value

    @property
    def channel(self):
        return self.location[self.channel_index]
    @channel.setter
    def channel(self, value):
        self.location[self.channel_index] = value


class FlashRequest(object):
    OP_READ, OP_WRITE, OP_ERASE = 'OP_READ', 'OP_WRITE', 'OP_ERASE'
    def __init__(self):
        self.addr = None
        self.operation = None

    def __str__(self):
        lines = []
        lines.append( "OPERATION " + str(self.operation) )
        lines.append( str(self.addr) )
        return '\n'.join(lines)


def display_flash_requests(requests):
    reqs = [str(req) for req in requests]
    print '\n'.join(reqs)


def create_flashrequest(addr, op):
    req = FlashRequest()
    req.addr = addr

    if op == 'read':
        req.operation = FlashRequest.OP_READ
    elif op == 'write':
        req.operation = FlashRequest.OP_WRITE
    elif op == 'erase':
        req.operation = FlashRequest.OP_ERASE
    else:
        raise RuntimeError()

    return req


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

        self.n_pages_per_plane = self.n_pages_per_block * self.n_blocks_per_plane
        self.n_pages_per_chip = self.n_pages_per_plane * self.n_planes_per_chip
        self.n_pages_per_package = self.n_pages_per_chip * self.n_chips_per_package
        self.n_pages_per_channel = self.n_pages_per_package * self.n_packages_per_channel
        self.n_pages_per_dev = self.n_pages_per_channel * self.n_channels_per_dev

        self.page_hierarchy = [self.n_pages_per_channel,
                                self.n_pages_per_package,
                                self.n_pages_per_chip,
                                self.n_pages_per_plane,
                                self.n_pages_per_block]

        self.channels = [Channel(self.env, conf, i)
                for i in range( self.n_channels_per_dev)]

    def physical_to_machine_page(self, page):
        addr = FlashAddress()

        no = page
        # page_hierarchy has [channel, package, ..., block]
        # location has       [channel, package, ..., block, page]
        for i, count in enumerate(self.page_hierarchy):
            addr.location[i] = no / count
            no = no % count
        addr.location[-1] = no

        return addr

    def get_flash_requests_for_pbn(self, block_start, block_count, op):
        ret_requests = []
        for block in range(block_start, block_start + block_count):
            machine_block_addr = self.physical_to_machine_block(block)
            flash_req = create_flashrequest( machine_block_addr, op = op)
            ret_requests.append(flash_req)

        return ret_requests

    def get_flash_requests_for_ppn(self, page_start, page_count, op):
        """
        op can be 'read', 'write', and 'erase'
        """
        ret_requests = []
        for page in range(page_start, page_start + page_count):
            machine_page_addr = self.physical_to_machine_page(page)
            flash_req = create_flashrequest(machine_page_addr, op = op)
            ret_requests.append(flash_req)

        return ret_requests

    def physical_to_machine_block(self, block):
        """
        We first translate block to the page number of its first page,
        so we can use the existing physical_to_machine_page
        """
        page = block * self.n_pages_per_block

        addr = self.physical_to_machine_page(page)
        addr.page = None # so we dont' mistakely use it for other purposes

        return addr

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

    def execute_request(self, flash_request):
        if flash_request.operation == FlashRequest.OP_READ:
            yield self.env.process(
                    self.read_page(flash_request.addr))
        elif flash_request.operation == FlashRequest.OP_WRITE:
            yield self.env.process(
                self.write_page(flash_request.addr))
        elif flash_request.operation == FlashRequest.OP_ERASE:
            yield self.env.process(
                self.erase_block(flash_request.addr))
        else:
            raise RuntimeError("operation {} is not supported".format(
                flash_request.operation))


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
    def __init__(self, simpy_env, conf, channel_id = None):
        self.env = simpy_env
        self.conf = conf
        self.resource = simpy.Resource(self.env, capacity = 1)
        self.channel_id = channel_id

        t_wc = 1
        t_r = 1
        t_rc = 1
        t_prog = 1
        t_erase = 1
        page_size = self.conf['flash_config']['page_size']

        # self.read_time = 7 * t_wc + t_r + page_size * t_rc
        # self.program_time = 7 * t_wc + page_size * t_wc + t_prog
        # self.erase_time = 5 * t_wc + t_erase

        self.read_time = 1
        self.program_time = 2
        self.erase_time = 3

    def write_page(self, addr = None , data = None):
        """
        If you want to when this operation is finished, just print env.now.
        If you want to know how long it takes, use env.now before and after
        the operation.
        """
        with self.resource.request() as request:
            yield request
            yield self.env.timeout( self.program_time )

    def read_page(self, addr = None):
        with self.resource.request() as request:
            yield request
            yield self.env.timeout( self.read_time )

    def erase_block(self, addr = None):
        with self.resource.request() as request:
            yield request
            yield self.env.timeout( self.erase_time )



