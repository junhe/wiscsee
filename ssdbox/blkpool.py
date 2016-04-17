from collections import deque

class BlockPool(object):
    def __init__(self, confobj):
        self.conf = confobj
        self.n_channels = self.conf['flash_config']['n_channels_per_dev']
        self.channel_pools = [ChannelBlockPool(self.conf, i)
                for i in range(self.n_channels)]

        self.cur_channel = 0
        self.stripe_size = self.conf['stripe_size']

    @property
    def freeblocks(self):
        free = []
        for channel in self.channel_pools:
            free.extend( channel.freeblocks_global )
        return free

    @property
    def data_usedblocks(self):
        used = []
        for channel in self.channel_pools:
            used.extend( channel.data_usedblocks_global )
        return used

    @property
    def trans_usedblocks(self):
        used = []
        for channel in self.channel_pools:
            used.extend( channel.trans_usedblocks_global )
        return used

    def _exec_on_channel(self, func_name, channel_id):
        """
        The type of in_channel_offset depends on the function you call
        """
        in_channel_offset = eval("self.channel_pools[channel_id].{}()"\
                .format(func_name))
        return in_channel_offset

    def _convert_offset(self, channel_id, in_channel_offset, addr_type):
        if addr_type == 'block':
            ret = channel_block_to_block(self.conf, channel_id,
                    in_channel_offset)
        elif addr_type == 'page':
            ret = channel_page_to_page(self.conf, channel_id,
                    in_channel_offset)
        else:
            raise RuntimeError("addr_type {} is not supported."\
                .format(addr_type))
        return ret

    def iter_channels(self, funcname, addr_type):
        n = self.n_channels
        while n > 0:
            n -= 1
            try:
                in_channel_offset = self._exec_on_channel(funcname,
                        self.cur_channel)
            except OutOfSpaceError:
                pass
            else:
                return self._convert_offset(self.cur_channel,
                        in_channel_offset, addr_type)
            finally:
                self.cur_channel = (self.cur_channel + 1) % self.n_channels

        raise OutOfSpaceError("Tried all channels. Out of Space")

    def pop_a_free_block(self):
        return self.iter_channels("pop_a_free_block", addr_type = 'block')

    def pop_a_free_block_to_trans(self):
        return self.iter_channels("pop_a_free_block_to_trans",
            addr_type = 'block')

    def pop_a_free_block_to_data(self):
        return self.iter_channels("pop_a_free_block_to_data",
            addr_type = 'block')

    def move_used_data_block_to_free(self, blocknum):
        channel, block_off = block_to_channel_block(self.conf, blocknum)
        self.channel_pools[channel].move_used_data_block_to_free(block_off)

    def move_used_trans_block_to_free(self, blocknum):
        channel, block_off = block_to_channel_block(self.conf, blocknum)
        self.channel_pools[channel].move_used_trans_block_to_free(block_off)

    def next_n_data_pages_to_program(self, n):
        ppns = []
        for i in range(n):
            ppns.append(self.next_data_page_to_program())

        return ppns

    def next_n_data_pages_to_program_striped(self, n):
        return self._next_n_data_pages_to_program_striped(n,
                self.stripe_size)

    def _next_n_data_pages_to_program_striped(self, n, stripe_size):
        """
        For example, n = 5, there are 4 channel, stripe size 2,
        then page 0,1 are in channel 0, page 2,3 are in channel 1,
        page 4 is in channel 2. Next request will be put to channel 2
        first.

        Stripe_size is in pages
        """
        n_pages_left = n
        ppns = []
        outofspace_channels = set()
        stipe_size = float('inf') if stripe_size == 'infinity' else stripe_size

        while n_pages_left > 0:
            needed_on_channel = min(stripe_size, n_pages_left)
            ppns_on_channel = self._grab_n_pages_on_channel(
                    n = needed_on_channel, channel_id = self.cur_channel,
                    page_type = 'data')

            ppns.extend(ppns_on_channel)

            #ppns_on_channel can be less than stripe size
            n_pages_left -= len(ppns_on_channel)
            if len(ppns_on_channel) == 0:
                outofspace_channels.add(self.cur_channel)

            if len(outofspace_channels) == self.n_channels:
                raise OutOfSpaceError

            self.cur_channel = (self.cur_channel + 1) % self.n_channels

        return ppns

    def _grab_n_pages_on_channel(self, n, channel_id, page_type):
        ppns = []
        for i in range(n):
            try:
                ppn = self._grab_1_page_on_channel(channel_id, page_type)
            except OutOfSpaceError:
                pass
            else:
                ppns.append(ppn)
        return ppns

    def _grab_1_page_on_channel(self, channel_id, page_type):
        if page_type == 'data':
            offset = self._exec_on_channel("next_data_page_to_program",
                channel_id)
        elif page_type == 'translation':
            offset = self._exec_on_channel(
                    "next_translation_page_to_program", channel_id)
        elif page_type == 'gc_data':
            offset = self._exec_on_channel(
                    "next_gc_data_page_to_program", channel_id)
        elif page_type == 'gc_translation':
            offset = self._exec_on_channel(
                    "next_gc_translation_page_to_program", channel_id)
        else:
            raise RuntimeError("page_type {} is not supported."\
                    .format(page_type))
        ppn = self._convert_offset(channel_id, offset, 'page')
        return ppn

    def next_data_page_to_program(self):
        return self.iter_channels("next_data_page_to_program",
            addr_type = 'page')

    def next_translation_page_to_program(self):
        return self.iter_channels("next_translation_page_to_program",
            addr_type = 'page')

    def next_gc_data_page_to_program(self):
        return self.iter_channels("next_gc_data_page_to_program",
            addr_type = 'page')

    def next_gc_translation_page_to_program(self):
        return self.iter_channels("next_gc_translation_page_to_program",
            addr_type = 'page')

    def current_blocks(self):
        cur_blocks = []
        for channel in self.channel_pools:
            cur_blocks.extend( channel.current_blocks_global )

        return cur_blocks

    def used_ratio(self):
        n_used = 0
        for channel in self.channel_pools:
            n_used += channel.total_used_blocks()

        return float(n_used) / self.conf.n_blocks_per_dev

    def total_used_blocks(self):
        total = 0
        for channel in self.channel_pools:
            total += channel.total_used_blocks()
        return total

    def num_freeblocks(self):
        total = 0
        for channel in self.channel_pools:
            total += len( channel.freeblocks )
        return total

class ChannelBlockPool(object):
    """
    This class maintains the free blocks and used blocks of a
    flash channel.
    The block number of each channel starts from 0.
    """
    def __init__(self, confobj, channel_no):
        self.conf = confobj

        self.freeblocks = deque(
            range(self.conf.n_blocks_per_channel))

        # initialize usedblocks
        self.trans_usedblocks = []
        self.data_usedblocks  = []

        self.channel_no = channel_no

    def shift_to_global(self, blocks):
        """
        calculate the block num in global namespace for blocks
        """
        return [ channel_block_to_block(self.conf, self.channel_no, block_off)
            for block_off in blocks ]

    @property
    def freeblocks_global(self):
        return self.shift_to_global(self.freeblocks)

    @property
    def trans_usedblocks_global(self):
        return self.shift_to_global(self.trans_usedblocks)

    @property
    def data_usedblocks_global(self):
        return self.shift_to_global(self.data_usedblocks)

    @property
    def current_blocks_global(self):
        local_cur_blocks = self.current_blocks()

        global_cur_blocks = []
        for block in local_cur_blocks:
            if block == None:
                global_cur_blocks.append(block)
            else:
                global_cur_blocks.append(
                    channel_block_to_block(self.conf, self.channel_no, block) )

        return global_cur_blocks

    def pop_a_free_block(self):
        if self.freeblocks:
            blocknum = self.freeblocks.popleft()
        else:
            # nobody has free block
            raise OutOfSpaceError('No free blocks in device!!!!')

        return blocknum

    def pop_a_free_block_to_trans(self):
        "take one block from freelist and add it to translation block list"
        blocknum = self.pop_a_free_block()
        self.trans_usedblocks.append(blocknum)
        return blocknum

    def pop_a_free_block_to_data(self):
        "take one block from freelist and add it to data block list"
        blocknum = self.pop_a_free_block()
        self.data_usedblocks.append(blocknum)
        return blocknum

    def move_used_data_block_to_free(self, blocknum):
        self.data_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def move_used_trans_block_to_free(self, blocknum):
        try:
            self.trans_usedblocks.remove(blocknum)
        except ValueError:
            sys.stderr.write( 'blocknum:' + str(blocknum) )
            raise
        self.freeblocks.append(blocknum)

    def total_used_blocks(self):
        return len(self.trans_usedblocks) + len(self.data_usedblocks)

    def next_page_to_program(self, log_end_name_str, pop_free_block_func):
        """
        The following comment uses next_data_page_to_program() as a example.

        it finds out the next available page to program
        usually it is the page after log_end_pagenum.

        If next=log_end_pagenum + 1 is in the same block with
        log_end_pagenum, simply return log_end_pagenum + 1
        If next=log_end_pagenum + 1 is out of the block of
        log_end_pagenum, we need to pick a new block from self.freeblocks

        This function is stateful, every time you call it, it will advance by
        one.
        """

        if not hasattr(self, log_end_name_str):
           # This is only executed for the first time
           cur_block = pop_free_block_func()
           # use the first page of this block to be the
           next_page = self.conf.block_off_to_page(cur_block, 0)
           # log_end_name_str is the page that is currently being operated on
           setattr(self, log_end_name_str, next_page)

           return next_page

        cur_page = getattr(self, log_end_name_str)
        cur_block, cur_off = self.conf.page_to_block_off(cur_page)

        next_page = (cur_page + 1) % self.conf.total_num_pages()
        next_block, next_off = self.conf.page_to_block_off(next_page)

        if cur_block == next_block:
            ret = next_page
        else:
            # get a new block
            block = pop_free_block_func()
            start, _ = self.conf.block_to_page_range(block)
            ret = start

        setattr(self, log_end_name_str, ret)
        return ret

    def next_data_page_to_program(self):
        return self.next_page_to_program('data_log_end_ppn',
            self.pop_a_free_block_to_data)

    def next_translation_page_to_program(self):
        return self.next_page_to_program('trans_log_end_ppn',
            self.pop_a_free_block_to_trans)

    def next_gc_data_page_to_program(self):
        return self.next_page_to_program('gc_data_log_end_ppn',
            self.pop_a_free_block_to_data)

    def next_gc_translation_page_to_program(self):
        return self.next_page_to_program('gc_trans_log_end_ppn',
            self.pop_a_free_block_to_trans)

    def current_blocks(self):
        try:
            cur_data_block, _ = self.conf.page_to_block_off(
                self.data_log_end_ppn)
        except AttributeError:
            cur_data_block = None

        try:
            cur_trans_block, _ = self.conf.page_to_block_off(
                self.trans_log_end_ppn)
        except AttributeError:
            cur_trans_block = None

        try:
            cur_gc_data_block, _ = self.conf.page_to_block_off(
                self.gc_data_log_end_ppn)
        except AttributeError:
            cur_gc_data_block = None

        try:
            cur_gc_trans_block, _ = self.conf.page_to_block_off(
                self.gc_trans_log_end_ppn)
        except AttributeError:
            cur_gc_trans_block = None

        return (cur_data_block, cur_trans_block, cur_gc_data_block,
            cur_gc_trans_block)

    def __repr__(self):
        ret = ' '.join(['freeblocks', repr(self.freeblocks)]) + '\n' + \
            ' '.join(['trans_usedblocks', repr(self.trans_usedblocks)]) + \
            '\n' + \
            ' '.join(['data_usedblocks', repr(self.data_usedblocks)])
        return ret

    def visual(self):
        block_states = [ 'O' if block in self.freeblocks else 'X'
            for block in range(self.conf.n_blocks_per_channel)]
        return ''.join(block_states)

    def used_ratio(self):
        return (len(self.trans_usedblocks) + len(self.data_usedblocks))\
            / float(self.conf.n_blocks_per_channel)


class OutOfSpaceError(RuntimeError):
    pass


def channel_page_to_page(conf, channel, page_off):
    """
    Translate channel, page_off to pagenum in context of device
    """
    return channel * conf.n_pages_per_channel + page_off


def page_to_channel_page(conf, pagenum):
    """
    pagenum is in the context of device
    """
    n_pages_per_channel = conf.n_pages_per_channel
    channel = pagenum / n_pages_per_channel
    page_off = pagenum % n_pages_per_channel
    return channel, page_off

def block_to_channel_block(conf, blocknum):
    n_blocks_per_channel = conf.n_blocks_per_channel
    channel = blocknum / n_blocks_per_channel
    block_off = blocknum % n_blocks_per_channel
    return channel, block_off

def channel_block_to_block(conf, channel, block_off):
    n_blocks_per_channel = conf.n_blocks_per_channel
    return channel * n_blocks_per_channel + block_off


