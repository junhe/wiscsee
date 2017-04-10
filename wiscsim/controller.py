import simpy
import wiscsim
from collections import Counter
from commons import *

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
    # OP_READ, OP_WRITE, OP_ERASE = 'OP_READ', 'OP_WRITE', 'OP_ERASE'
    def __init__(self):
        self.addr = None
        self.operation = None

    def __str__(self):
        lines = []
        lines.append( "OPERATION " + str(self.operation) )
        lines.append( str(self.addr) )
        return '\n'.join(lines)


def create_flashrequest(addr, op):
    req = FlashRequest()
    req.addr = addr

    if op == 'read':
        req.operation = OP_READ
    elif op == 'write':
        req.operation = OP_WRITE
    elif op == 'erase':
        req.operation = OP_ERASE
    else:
        raise RuntimeError()

    return req


class Controller(object):
    """
    This base class implements the core functions of a flash controller.
    It should not have those side functions like recording
    """
    def __init__(self, simpy_env, conf):
        self.env = simpy_env
        self.conf = conf

        # TODO: should these be in config?
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

    def get_flash_requests_for_pbns(self, block_start, block_count, op):
        """
        Mapping pbn seen by FTL to hierarchical address used by flash
        controller
        """
        ret_requests = []
        for block in range(block_start, block_start + block_count):
            machine_block_addr = self.physical_to_machine_block(block)
            flash_req = create_flashrequest( machine_block_addr, op = op)
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

    def get_flash_requests_for_ppns(self, page_start, page_count, op):
        """
        op can be 'read', 'write', and 'erase'
        """
        ret_requests = []
        for page in range(page_start, page_start + page_count):
            machine_page_addr = self.physical_to_machine_page(page)
            flash_req = create_flashrequest(machine_page_addr, op = op)
            ret_requests.append(flash_req)

        return ret_requests

    def physical_to_machine_page(self, page_no):
        addr = FlashAddress()

        # page_hierarchy has [channel, package, ..., block]
        # location has       [channel, package, ..., block, page]
        for i, count in enumerate(self.page_hierarchy):
            addr.location[i] = page_no / count
            page_no = page_no % count
        addr.location[-1] = page_no

        return addr

    def rw_ppn_extent(self, ppn_start, ppn_count, op):
        """
        op is 'read' or 'write'
        """
        flash_reqs = self.get_flash_requests_for_ppns(ppn_start, ppn_count,
            op = op)
        yield self.env.process( self.execute_request_list(flash_reqs) )

    def erase_pbn_extent(self, pbn_start, pbn_count):
        flash_reqs = self.get_flash_requests_for_pbns(pbn_start, pbn_count,
                op = 'erase')
        yield self.env.process( self.execute_request_list(flash_reqs) )

    def execute_request_list(self, flash_request_list):
        procs = []
        for request in flash_request_list:
            p = self.env.process(self.execute_request(request))
            procs.append(p)
        event = simpy.events.AllOf(self.env, procs)
        yield event

    def execute_request(self, flash_request):
        if flash_request.operation == OP_READ:
            yield self.env.process(
                    self.read_page(flash_request.addr))
        elif flash_request.operation == OP_WRITE:
            yield self.env.process(
                self.write_page(flash_request.addr))
        elif flash_request.operation == OP_ERASE:
            yield self.env.process(
                self.erase_block(flash_request.addr))
        else:
            raise RuntimeError("operation {} is not supported".format(
                flash_request.operation))

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


class Controller3(Controller):
    """
    With tag, and recorder
    """
    def __init__(self, simpy_env, conf, recorderobj):
        super(Controller3, self).__init__(simpy_env, conf)

        self.recorder = recorderobj
        self.channels = [Channel3(self.env, conf, self.recorder, i)
                for i in range( self.n_channels_per_dev)]

    def execute_request_list(self, flash_request_list, tag):
        procs = []
        for request in flash_request_list:
            p = self.env.process(self.execute_request(request, tag))
            procs.append(p)
        event = simpy.events.AllOf(self.env, procs)
        yield event

    def write_page(self, addr, tag, data = None):
        yield self.env.process(
            self.channels[addr.channel].write_page(tag = tag,
                addr = None, data = None))

    def read_page(self, addr, tag):
        yield self.env.process(
            self.channels[addr.channel].read_page(tag = tag,
                addr = None))

    def erase_block(self, addr, tag):
        yield self.env.process(
            self.channels[addr.channel].erase_block(tag = tag, addr = None))

    def rw_ppns(self, ppns, op, tag):
        procs = []
        for ppn in ppns:
            p = self.env.process(
                    self.rw_ppn_extent(ppn, 1, op, tag))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

    def rw_ppn_extent(self, ppn_start, ppn_count, op, tag):
        """
        op is 'read' or 'write'
        """
        flash_reqs = self.get_flash_requests_for_ppns(ppn_start, ppn_count,
            op = op)
        yield self.env.process( self.execute_request_list(flash_reqs, tag) )

    def erase_pbn_extent(self, pbn_start, pbn_count, tag):
        flash_reqs = self.get_flash_requests_for_pbns(pbn_start, pbn_count,
                op = 'erase')
        yield self.env.process( self.execute_request_list(flash_reqs, tag) )

    def execute_request(self, flash_request, tag):
        self.recorder.count_me('flash_ops', flash_request.operation)
        if flash_request.operation == OP_READ:
            yield self.env.process(
                    self.read_page(addr = flash_request.addr, tag = tag))
        elif flash_request.operation == OP_WRITE:
            yield self.env.process(
                self.write_page(flash_request.addr, tag = tag))
        elif flash_request.operation == OP_ERASE:
            yield self.env.process(
                self.erase_block(flash_request.addr, tag = tag))
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

        self.read_time = 7 * self.conf['flash_config']['t_WC'] + \
            self.conf['flash_config']['t_R'] + \
            self.conf['flash_config']['page_size'] * \
            self.conf['flash_config']['t_RC']
        self.program_time = 7 * self.conf['flash_config']['t_WC'] + \
            self.conf['flash_config']['page_size'] * \
            self.conf['flash_config']['t_WC'] +\
            self.conf['flash_config']['t_PROG']
        self.erase_time = 5 * self.conf['flash_config']['t_WC'] + \
            self.conf['flash_config']['t_BERS']

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

class Channel2(Channel):
    """
    It has recorder
    """
    def __init__(self, simpy_env, conf, recorderobj, channel_id = None):
        super(Channel2, self).__init__(simpy_env, conf, channel_id)
        self.recorder = recorderobj

    def write_page(self, addr = None , data = None):
        """
        If you want to when this operation is finished, just print env.now.
        If you want to know how long it takes, use env.now before and after
        the operation.
        """
        with self.resource.request() as request:
            yield request
            s = self.env.now
            yield self.env.timeout( self.program_time )
            e = self.env.now
            self.recorder.add_to_timer("channel_busy_time", self.channel_id,
                    e - s)

    def read_page(self, addr = None):
        with self.resource.request() as request:
            yield request
            s = self.env.now
            yield self.env.timeout( self.read_time )
            e = self.env.now
            self.recorder.add_to_timer("channel_busy_time", self.channel_id,
                    e - s)

    def erase_block(self, addr = None):
        with self.resource.request() as request:
            yield request
            s = self.env.now
            yield self.env.timeout( self.erase_time )
            e = self.env.now
            self.recorder.add_to_timer("channel_busy_time", self.channel_id,
                    e - s)


class Channel3(Channel2):
    """
    Operations can be tagged
    """
    def counter_set_name(self):
        return "channel_busy_time"

    def _convert_tag(self, tag):
        if isinstance(tag, dict):
            return tag
        elif isinstance(tag, str):
            return {'tag':tag}

    def _write_channel_timeline(self, channel_id, start_time, end_time, tag):
        write = self.conf.get("write_channel_timeline", False)
        if write is True:
            tag = self._convert_tag(tag)
            self.recorder.write_file('channel_timeline.txt',
                channel=channel_id, start_time=start_time, end_time=end_time,
                **tag)

    def write_page(self, tag, addr = None , data = None):
        """
        If you want to when this operation is finished, just print env.now.
        If you want to know how long it takes, use env.now before and after
        the operation.
        """
        with self.resource.request() as request:
            yield request
            s = self.env.now
            yield self.env.timeout( self.program_time )
            e = self.env.now
            self.recorder.add_to_timer(
                self.counter_set_name(),
                "channel_{id}-write-{tag}".format(id = self.channel_id,
                    tag = self.recorder.tag_group(tag)),
                e - s)
            self._write_channel_timeline(channel_id=self.channel_id,
                    start_time=s, end_time=e, tag=tag)

    def read_page(self, tag, addr = None):
        with self.resource.request() as request:
            yield request
            s = self.env.now
            yield self.env.timeout( self.read_time )
            e = self.env.now
            self.recorder.add_to_timer(
                self.counter_set_name(),
                "channel_{id}-read-{tag}".format(id = self.channel_id,
                    tag = self.recorder.tag_group(tag)),
                e - s)
            self._write_channel_timeline(channel_id=self.channel_id,
                    start_time=s, end_time=e, tag=tag)

    def erase_block(self, tag, addr = None):
        with self.resource.request() as request:
            yield request
            s = self.env.now
            yield self.env.timeout( self.erase_time )
            e = self.env.now
            self.recorder.add_to_timer(
                self.counter_set_name(),
                "channel_{id}-erase-{tag}".format(id = self.channel_id,
                    tag = self.recorder.tag_group(tag)),
                e - s)
            self._write_channel_timeline(channel_id=self.channel_id,
                    start_time=s, end_time=e, tag=tag)

