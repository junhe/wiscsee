import bitarray
from collections import deque, Counter
import csv
import datetime
import itertools
import random
import os
import Queue
import sys
import simpy

import bidict

import config
import flash
import ftlbuilder
import lrulist
import recorder
import utils
from commons import *
from ftlsim_commons import *


UNINITIATED, MISS = ('UNINIT', 'MISS')
DATA_BLOCK, TRANS_BLOCK = ('data_block', 'trans_block')
random.seed(0)
LOGICAL_READ, LOGICAL_WRITE, LOGICAL_DISCARD = ('LOGICAL_READ', \
        'LOGICAL_WRITE', 'LOGICAL_DISCARD')
#
# - translation pages
#   - cache miss read (trans.cache.load)
#   - eviction write  (trans.cache.evict)
#   - cleaning read   (trans.clean)
#   - cleaning write  (trans.clean)
# - data pages
#   - user read       (data.user)
#   - user write      (data.user)
#   - cleaning read   (data.cleaning)
#   - cleaning writes (data.cleaning)
# Tag format
# pagetype.
# Example tags:

# trans cache read is due to cache misses, the read fetches translation page
# to cache.
# write is due to eviction. Note that entry eviction may incure both page read
# and write.
TRANS_CACHE = "trans.cache"

# trans clean include:
#  erasing translation block
#  move translation page during gc (including read and write)
TRANS_CLEAN = "trans.clean"  #read/write are for moving pages

#  clean_data_block()
#   update_mapping_in_batch()
#    update_translation_page_on_flash() this is the same as cache eviction
TRANS_UPDATE_FOR_DATA_GC = "trans.update.for.data.gc"

DATA_USER = "data.user"

# erase data block in clean_data_block()
# move data page during gc (including read and write)
DATA_CLEANING = "data.cleaning"


class Dftl(object):
    """
    The implementation literally follows DFtl paper.
    This class is a coordinator of other coordinators and data structures
    """
    def __init__(self, confobj, recorderobj, flashcontrollerobj, env):
        if not isinstance(confobj, Config):
            raise TypeError("confobj is not Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj
        self.recorder = recorderobj
        self.flash = flashcontrollerobj
        self.env = env

        self.global_helper = GlobalHelper(confobj)

        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)

        ###### the managers ######
        self.mapping_manager = MappingManager(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = self.flash,
            oobobj=self.oob,
            recorderobj = recorderobj,
            envobj = env
            )

        self.garbage_collector = GarbageCollector(
            confobj = self.conf,
            flashobj = self.flash,
            oobobj=self.oob,
            block_pool = self.block_pool,
            mapping_manager = self.mapping_manager,
            recorderobj = recorderobj,
            envobj = env
            )

        self.n_sec_per_page = self.conf.page_size \
                / self.conf['sector_size']

        # This resource protects all data structures stored in the memory.
        self.resource_ram = simpy.Resource(self.env, capacity = 1)

    def translate(self, ssd_req, pid):
        """
        Our job here is to find the corresponding physical flat address
        of the address in ssd_req. The during the translation, we may
        need to synchronizely access flash.

        We do the following here:
        Read:
            1. find out the range of logical pages
            2. for each logical page:
                if mapping is in cache, just translate it
                else bring mapping to cache and possibly evict a mapping entry
        """
        with self.resource_ram.request() as ram_request:
            yield ram_request # serialize all access to data structures

            s = self.env.now
            flash_reqs = yield self.env.process(
                    self.handle_ssd_requests(ssd_req))
            e = self.env.now
            self.recorder.add_to_timer("translation_time-wo_wait", pid, e - s)

            self.env.exit(flash_reqs)

    def handle_ssd_requests(self, ssd_req):
        lpns = ssd_req.lpn_iter()

        if ssd_req.operation == 'read':
            ppns = yield self.env.process(
                    self.mapping_manager.ppns_for_reading(lpns))
        elif ssd_req.operation == 'write':
            ppns = yield self.env.process(
                    self.mapping_manager.ppns_for_writing(lpns))
        elif ssd_req.operation == 'discard':
            yield self.env.process(
                    self.mapping_manager.discard_lpns(lpns))
            ppns = []
        else:
            print 'io operation', ssd_req.operation, 'is not processed'
            ppns = []

        flash_reqs = []
        for ppn in ppns:
            if ppn == 'UNINIT':
                continue

            req = self.flash.get_flash_requests_for_ppns(ppn, 1,
                    op = ssd_req.operation)
            flash_reqs.extend(req)

        self.env.exit(flash_reqs)

    def clean_garbage(self):
        with self.resource_ram.request() as ram_request:
            yield ram_request # serialize all access to data structures
            yield self.env.process(
                    self.garbage_collector.gc_process())


    def page_to_sec_items(self, data):
        ret = []
        for page_data in data:
            if page_data == None:
                page_data = [None] * self.n_sec_per_page
            for item in page_data:
                ret.append(item)

        return ret

    def sec_to_page_items(self, data):
        if data == None:
            return None

        sec_per_page = self.conf.page_size / self.conf['sector_size']
        n_pages = len(data) / sec_per_page

        new_data = []
        for page in range(n_pages):
            page_items = []
            for sec in range(sec_per_page):
                page_items.append(data[page * sec_per_page + sec])
            new_data.append(page_items)

        return new_data

    def pre_workload(self):
        pass

    def post_processing(self):
        """
        This function is called after the simulation.
        """
        pass

    def get_type(self):
        return "dftldes"


class Ftl(object):
    def __init__(self, confobj, recorderobj, flashcontrollerobj, env):
        if not isinstance(confobj, Config):
            raise TypeError("confobj is not Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj
        self.recorder = recorderobj
        self.flash = flashcontrollerobj
        self.env = env

        self.global_helper = GlobalHelper(confobj)

        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)

        self._directory = GlobalTranslationDirectory(self.conf,
                self.oob, self.block_pool)

        self._mappings = MappingCache(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = self.flash,
            oobobj = self.oob,
            recorderobj = self.recorder,
            envobj = self.env,
            directory = self._directory,
            mapping_on_flash = MappingOnFlash(self.conf))

        self.n_sec_per_page = self.conf.page_size \
                / self.conf['sector_size']

    def write_ext(self, extent):
        ext_list = split_ext_to_mvpngroups(self.conf, extent)

        procs = []
        for ext_single_m_vpn in ext_list:
            p = self.env.process(self._write_single_mvpngroup(ext_single_m_vpn))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

    def _write_single_mvpngroup(self, ext_single_m_vpn):
        m_vpn = self.conf.lpn_to_m_vpn(ext_single_m_vpn.lpn_start)

        ppns_to_write = self.block_pool.next_n_data_pages_to_program(
                ext_single_m_vpn.lpn_count)
        old_ppns = yield self.env.process(
                self._mappings.lpns_to_ppns(ext_single_m_vpn.lpn_iter()))

        yield self.env.process(
            self._update_metadata_for_relocating_lpns(ext_single_m_vpn.lpn_iter(),
                old_ppns = old_ppns, new_ppns = ppns_to_write))

        yield self.env.process(
            self.flash.rw_ppns(ppns_to_write, 'write', tag = 'TAG_FOREGROUND'))

    def _update_metadata_for_relocating_lpns(self, lpns, old_ppns, new_ppns):
        for lpn, old_ppn, new_ppn in zip(lpns, old_ppns, new_ppns):
            yield self.env.process(
                self._update_metadata_for_relocating_lpn(lpn, old_ppn, new_ppn))

    def _update_metadata_for_relocating_lpn(self, lpn, old_ppn, new_ppn):
        """
        contents of lpn used to be in old_ppn, but now it is in new_ppn.
        This function adjust all metadata to reflect the change.

        ----- template for metadata change --------
        # mappings in cache
        raise NotImplementedError()

        # mappings on flash
        raise NotImplementedError()

        # translation directory
        raise NotImplementedError()

        # oob state
        raise NotImplementedError()

        # oob ppn->lpn/vpn
        raise NotImplementedError()

        # blockpool
        raise NotImplementedError()
        """
        # mappings in cache
        yield self.env.process(
                self._mappings.update(lpn = lpn, ppn = new_ppn))

        # mappings on flash
        #   handled by _mappings

        # translation directory
        #   handled by _mappings

        # oob state
        # oob ppn->lpn/vpn
        self.oob.new_lba_write(lpn = lpn, old_ppn = old_ppn,
            new_ppn = new_ppn)

        # blockpool
        #   should be handled when we got new_ppn

    def read_ext(self, extent):
        ext_list = split_ext_to_mvpngroups(self.conf, extent)
        # print [str(x) for x in ext_list]

        procs = []
        for ext_single_m_vpn in ext_list:
            p = self.env.process(self._read_single_mvpngroup(ext_single_m_vpn))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

    def _read_single_mvpngroup(self, ext_single_m_vpn):
        m_vpn = self.conf.lpn_to_m_vpn(ext_single_m_vpn.lpn_start)

        ppns_to_read = yield self.env.process(
                self._mappings.lpns_to_ppns(ext_single_m_vpn.lpn_iter()))
        ppns_to_read = remove_invalid_ppns(ppns_to_read)

        yield self.env.process(
            self.flash.rw_ppns(ppns_to_read, 'read', tag = 'TAG_FOREGROUND'))

    def discard_ext(self, extent):
        ext_list = split_ext_to_mvpngroups(self.conf, extent)

        procs = []
        for ext_single_m_vpn in ext_list:
            p = self.env.process(self._discard_single_mvpngroup(ext_single_m_vpn))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

    def _discard_single_mvpngroup(self, ext_single_m_vpn):
        m_vpn = self.conf.lpn_to_m_vpn(ext_single_m_vpn.lpn_start)

        ppns_to_invalidate = yield self.env.process(
                self._mappings.lpns_to_ppns(ext_single_m_vpn.lpn_iter()))

        ppns_to_invalidate = remove_invalid_ppns(ppns_to_invalidate)

        mapping_dict = dict(itertools.izip_longest(
            ppns_to_invalidate, (), fillvalue = UNINITIATED))
        self._mappings.update_batch(mapping_dict)

        self.oob.wipe_ppns(ppns_to_invalidate)


def remove_invalid_ppns(ppns):
    return [ppn for ppn in ppns if not ppn in (UNINITIATED, MISS)]

def split_ext_to_mvpngroups(conf, extent):
    """
    return a list of extents, each belongs to one m_vpn
    """
    group_extent_list = []
    for i, lpn in enumerate(extent.lpn_iter()):
        cur_m_vpn = conf.lpn_to_m_vpn(lpn)
        if i == 0:
            # intialization
            cur_group_extent = Extent(lpn_start = extent.lpn_start,
                lpn_count = 1)
            group_extent_list.append(cur_group_extent)
        else:
            if cur_m_vpn == last_m_vpn:
                cur_group_extent.lpn_count += 1
            else:
                cur_group_extent = Extent(lpn_start = lpn,
                    lpn_count = 1)
                group_extent_list.append(cur_group_extent)

        last_m_vpn = cur_m_vpn

    return group_extent_list


class GlobalHelper(object):
    """
    In case you need some global variables. We put all global stuff here so
    it is easier to manage. (And you know all the bad things you did :)
    """
    def __init__(self, confobj):
        pass

class VPNResourcePool(object):
    def __init__(self, simpy_env):
        self.resources = {} # lpn: lock
        self.env = simpy_env

    def get_request(self, vpn):
        res = self.resources.setdefault(vpn,
                                    simpy.Resource(self.env, capacity = 1))
        return res.request()

    def release_request(self, vpn, request):
        res = self.resources[vpn]
        res.release(request)

class BlockPool(object):
    def __init__(self, confobj):
        self.conf = confobj
        self.n_channels = self.conf['flash_config']['n_channels_per_dev']
        self.channel_pools = [ChannelBlockPool(self.conf, i)
                for i in range(self.n_channels)]

        self.cur_channel = 0

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

    def iter_channels(self, funcname, addr_type):
        n = self.n_channels
        while n > 0:
            n -= 1
            try:
                in_channel_offset = eval("self.channel_pools[self.cur_channel].{}()"\
                        .format(funcname))
            except OutOfSpaceError:
                pass
            else:
                if addr_type == 'block':
                    ret = channel_block_to_block(self.conf, self.cur_channel,
                            in_channel_offset)
                elif addr_type == 'page':
                    ret = channel_page_to_page(self.conf, self.cur_channel,
                            in_channel_offset)
                else:
                    raise RuntimeError("addr_type {} is not supported."\
                        .format(addr_type))
                return ret
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


class MappingManager(object):
    """
    This class is the supervisor of all the mappings. When initializing, it
    register CMT and GMT with it and provides higher level operations on top of
    them.
    This class should act as a coordinator of all the mapping data structures.
    """
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj,
            envobj):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj
        self.env = envobj

        # managed and owned by Mappingmanager
        self.mapping_on_flash = MappingOnFlash(confobj)
        self.mapping_table = MappingTable(confobj)
        self.directory = GlobalTranslationDirectory(confobj, oobobj,
                block_pool)

    def ppns_for_writing(self, lpns):
        """
        This function returns ppns that can be written.

        The ppns returned are mapped by lpns, one to one
        """
        ppns = []
        for lpn in lpns:
            old_ppn = yield self.env.process( self.lpn_to_ppn(lpn) )
            new_ppn = self.block_pool.next_data_page_to_program()
            ppns.append(new_ppn)
            # CMT
            # lpn must be in cache thanks to self.mapping_manager.lpn_to_ppn()
            self.mapping_table.overwrite_entry(
                lpn = lpn, ppn = new_ppn, dirty = True)
            # OOB
            self.oob.new_lba_write(lpn = lpn, old_ppn = old_ppn,
                new_ppn = new_ppn)

        self.env.exit(ppns)

    def ppns_for_reading(self, lpns):
        """
        """
        ppns = []
        for lpn in lpns:
            ppn = yield self.env.process( self.lpn_to_ppn(lpn) )
            ppns.append(ppn)

        self.env.exit(ppns)

    def lpn_to_ppn(self, lpn):
        """
        This method does not fail. It will try everything to find the ppn of
        the given lpn.
        return: real PPN or UNINITIATED
        """
        # try cached mapping table first.
        ppn = self.mapping_table.lpn_to_ppn(lpn)
        if ppn == MISS:
            # cache miss
            while self.mapping_table.is_full():
                yield self.env.process(self.evict_cache_entry())

            # find the physical translation page holding lpn's mapping in GTD
            ppn = yield self.env.process(
                    self.load_mapping_entry_to_cache(lpn))

            self.recorder.count_me("cache", "miss")
        else:
            self.recorder.count_me("cache", "hit")

        self.env.exit(ppn)

    def load_mapping_entry_to_cache(self, lpn):
        """
        When a mapping entry is not in cache, you need to read the entry from
        flash and put it to cache. This function does this.
        Output: it return the ppn of lpn read from entry on flash.
        """
        # find the location of the translation page
        m_ppn = self.directory.lpn_to_m_ppn(lpn)

        # read it up, this operation is just for statistics
        yield self.env.process(
                self.flash.rw_ppn_extent(m_ppn, 1, op = 'read',
                tag = TAG_BACKGROUND))

        if self.conf.keeping_all_tp_entries == True:
            m_vpn = self.conf.lpn_to_m_vpn(lpn)
            entries = self.retrieve_translation_page(m_vpn)
            self.mapping_table.add_new_entries_if_not_exist(entries,
                    dirty = False)
            ppn = entries[lpn]
        else:
            # Now we have all the entries of m_ppn in memory, we need to put
            # the mapping of lpn->ppn to CMT
            ppn = self.mapping_on_flash.lpn_to_ppn(lpn)
            self.mapping_table.add_new_entry(lpn = lpn, ppn = ppn,
                dirty = False)

        self.env.exit(ppn)

    def retrieve_translation_page(self, m_vpn):
        """
        m_vpn is mapping virtual page number

        This function does not actually read flash. It just gets the mappings
        from global mapping table.
        """
        entries = {}
        for lpn in self.conf.m_vpn_to_lpns(m_vpn):
            entries[lpn] = self.mapping_on_flash.lpn_to_ppn(lpn)

        return entries

    def update_entry(self, lpn, new_ppn, tag = "NA"):
        """
        Update mapping of lpn to be lpn->new_ppn everywhere if necessary.

        if lpn is not in cache, it will NOT be added to it.

        block_pool:
            it may be affect because we need a new page
        CMT:
            if lpn is in cache, we need to update it and mark it as clean
            since after this function the cache will be consistent with GMT
        GMT:
            we need to read the old translation page, update it and write it
            to a new flash page
        OOB:
            we need to wipe out the old_ppn and fill the new_ppn
        GTD:
            we need to update m_vpn to new m_ppn
        """
        cached_ppn = self.mapping_table.lpn_to_ppn(lpn)
        if cached_ppn != MISS:
            # in cache
            self.mapping_table.overwrite_entry(lpn = lpn,
                ppn = new_ppn, dirty = False)

        m_vpn = self.conf.lpn_to_m_vpn(lpn)

        # batch_entries may be empty
        batch_entries = self.dirty_entries_of_translation_page(m_vpn)

        new_mappings = {lpn:new_ppn} # lpn->new_ppn may not be in cache
        for entry in batch_entries:
            new_mappings[entry.lpn] = entry.ppn

        # update translation page
        yield self.env.process(
            self.update_translation_page_on_flash(m_vpn, new_mappings, tag))

        # mark as clean
        for entry in batch_entries:
            entry.dirty = False

    def evict_cache_entry(self):
        """
        Select one entry in cache
        If the entry is dirty, write it back to GMT.
        If it is not dirty, simply remove it.
        """
        self.recorder.count_me('cache', 'evict')

        vic_lpn, vic_entrydata = self.mapping_table.victim_entry()

        if vic_entrydata.dirty == True:
            # If we have to write to flash, we write in batch
            m_vpn = self.conf.lpn_to_m_vpn(vic_lpn)
            yield self.env.process(self.batch_write_back(m_vpn))

        # remove only the victim entry
        self.mapping_table.remove_entry_by_lpn(vic_lpn)

    def batch_write_back(self, m_vpn):
        """
        Write dirty entries in a translation page with a flash read and a flash write.
        """
        self.recorder.count_me('cache', 'batch_write_back')

        batch_entries = self.dirty_entries_of_translation_page(m_vpn)

        new_mappings = {}
        for entry in batch_entries:
            new_mappings[entry.lpn] = entry.ppn

        # update translation page
        self.recorder.count_me('batch.size', len(new_mappings))
        yield self.env.process(
                self.update_translation_page_on_flash(m_vpn, new_mappings,
                    TRANS_CACHE))

        # mark them as clean
        for entry in batch_entries:
            entry.dirty = False

    def dirty_entries_of_translation_page(self, m_vpn):
        """
        Get all dirty entries in translation page m_vpn.
        """
        retlist = []
        for entry_lpn, dataentry in self.mapping_table.entries.items():
            if dataentry.dirty == True:
                tmp_m_vpn = self.conf.lpn_to_m_vpn(entry_lpn)
                if tmp_m_vpn == m_vpn:
                    retlist.append(dataentry)

        return retlist

    def update_translation_page_on_flash(self, m_vpn, new_mappings, tag):
        """
        Use new_mappings to replace their corresponding mappings in m_vpn

        read translationo page
        modify it with new_mappings
        write translation page to new location
        update related data structures

        Notes:
        - Note that it does not modify cached mapping table
        """
        old_m_ppn = self.directory.m_vpn_to_m_ppn(m_vpn)

        # update GMT on flash
        if len(new_mappings) < self.conf.n_mapping_entries_per_page:
            # need to read some mappings
            yield self.env.process(
                self.flash.rw_ppn_extent(old_m_ppn, 1, op = 'read',
                    tag = TAG_BACKGROUND) )
        else:
            self.recorder.count_me('cache', 'saved.1.read')

        pass # modify in memory. Since we are a simulator, we don't do anything
        new_m_ppn = self.block_pool.next_translation_page_to_program()

        # update flash
        yield self.env.process(
            self.flash.rw_ppn_extent(new_m_ppn, 1, op = 'write',
                tag = TAG_BACKGROUND))

        # update our fake 'on-flash' GMT
        for lpn, new_ppn in new_mappings.items():
            self.mapping_on_flash.update(lpn = lpn, ppn = new_ppn)

        # OOB, keep m_vpn as lpn
        self.oob.new_write(lpn = m_vpn, old_ppn = old_m_ppn,
            new_ppn = new_m_ppn)

        # update GTD so we can find it
        self.directory.update_mapping(m_vpn = m_vpn, m_ppn = new_m_ppn)

    def discard_lpn(self, lpn):
        ppn = yield self.env.process(self.lpn_to_ppn(lpn))
        if ppn == UNINITIATED:
            return

        # flash page ppn has valid data
        self.mapping_table.overwrite_entry(lpn = lpn,
            ppn = UNINITIATED, dirty = True)

        # OOB
        self.oob.wipe_ppn(ppn)

    def discard_lpns(self, lpns):
        for lpn in lpns:
            yield self.env.process(self.discard_lpn(lpn))


    def __translate_lpns_by_cache(self, lpns):
        """
        Return: Found all?, mappings
        """
        mappings = MappingDict()
        for lpn in lpns:
            ppn = self.mapping_table.lpn_to_ppn(lpn)
            if ppn == MISS:
                return False, None
            mappings[lpn] = ppn

        return True, mappings

    def translate_lpns_of_single_m_vpn(self, lpns):
        """
        lpns must be in the same m_vpn
        """
        found, mappings = self.__translate_lpns_by_cache(lpns)

        if found == True:
            return mappings

        # not in cache, we now need to make some room in cache for new
        # mappings

class MappingDict(dict):
    """
    Used to map lpn->ppn
    """
    pass

def block_to_channel_block(conf, blocknum):
    n_blocks_per_channel = conf.n_blocks_per_channel
    channel = blocknum / n_blocks_per_channel
    block_off = blocknum % n_blocks_per_channel
    return channel, block_off

def channel_block_to_block(conf, channel, block_off):
    n_blocks_per_channel = conf.n_blocks_per_channel
    return channel * n_blocks_per_channel + block_off

def page_to_channel_page(conf, pagenum):
    """
    pagenum is in the context of device
    """
    n_pages_per_channel = conf.n_pages_per_channel
    channel = pagenum / n_pages_per_channel
    page_off = pagenum % n_pages_per_channel
    return channel, page_off

def channel_page_to_page(conf, channel, page_off):
    """
    Translate channel, page_off to pagenum in context of device
    """
    return channel * conf.n_pages_per_channel + page_off


class MappingCache(object):
    """
    This class maintains MappingTable, it evict entries from MappingTable, load
    entries from flash.

    TODO: should separate operations that do/do not change recency
    """
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj,
            envobj, directory, mapping_on_flash):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj
        self.env = envobj
        self.directory = directory
        self.mapping_on_flash = mapping_on_flash

        self._cached_mappings = MappingTable(confobj)
        self.vpn_res_pool =  VPNResourcePool(self.env)
        self._load_lock = simpy.Resource(self.env, capacity = 1)

    def lpn_to_ppn(self, lpn):
        """
        Note that the return can be UNINITIATED
        TODO: avoid thrashing!
        """
        ppn = self._cached_mappings.lpn_to_ppn(lpn)
        if ppn == MISS:
            m_vpn = self.conf.lpn_to_m_vpn(lpn)
            yield self.env.process(self._load(m_vpn))
            ppn = self._cached_mappings.lpn_to_ppn(lpn)
            assert ppn != MISS

        self.env.exit(ppn)

    def lpns_to_ppns(self, lpns):
        """
        If lpns are of the same m_vpn, this process will only
        have one cache miss.
        """
        ppns = []
        for lpn in lpns:
            ppn = yield self.env.process(self.lpn_to_ppn(lpn))
            ppns.append(ppn)
        self.env.exit(ppns)

    def update(self, lpn, ppn):
        """
        It may evict to make room for this entry
        """
        if self._cached_mappings.has_lpn(lpn):
            # hit
            self._cached_mappings.overwrite_entry(lpn, ppn, dirty = True)
        else:
            # miss
            yield self.env.process(self._insert_new_mapping(lpn, ppn))

    def update_batch(self, mapping_dict):
        for lpn, ppn in mapping_dict.items():
            yield self.env.process(self.update(lpn, ppn))

    def _insert_new_mapping(self, lpn, ppn):
        """
        lpn should not be in cache
        """
        assert not self._cached_mappings.has_lpn(lpn)

        if self._cached_mappings.is_full():
            yield self.env.process(self._make_room(n_needed = 1))
        self._cached_mappings.add_new_entry(
            lpn = lpn, ppn = ppn, dirty = True)

        assert not self._cached_mappings.is_overflowed()

    def _make_room(self, n_needed):
        while self._cached_mappings.count_of_free() < n_needed:
            yield self.env.process(self._free_one_entry())

    def _free_one_entry(self):
        victim_lpn, entry_data = self._cached_mappings.victim_entry()
        if entry_data.dirty == True:
            m_vpn = self.conf.lpn_to_m_vpn(lpn = victim_lpn)
            yield self.env.process(self._write_back(m_vpn))

        self._cached_mappings.remove_entry_by_lpn(victim_lpn)

    def _load(self, m_vpn):
        # TODO: entries loaded from trans page should not change recency

        # Need a lock here because another thread may come in and think
        # the cache has enough space for it. But the space is for this load
        # TODO: somehow remove the lock to allow more parallelism
        load_req = self._load_lock.request()
        yield load_req

        n_needed = self._cached_mappings.needed_space_for_m_vpn(m_vpn)
        yield self.env.process(self._make_room(n_needed))

        yield self.env.process(self._load_to_free_space(m_vpn))

        self._load_lock.release(load_req)

    def _load_to_free_space(self, m_vpn):
        """
        It should not call _write_back() directly or indirectly as it
        will deadlock.
        """
        # aquire lock
        m_vpn_req = self.vpn_res_pool.get_request(m_vpn)
        yield m_vpn_req # should take no time, usually

        mapping_dict = yield self.env.process(
                self._read_translation_page(m_vpn))
        self._cached_mappings.add_new_entries_if_not_exist(mapping_dict,
                dirty = False)

        assert not self._cached_mappings.is_overflowed()

        # release lock
        self.vpn_res_pool.release_request(m_vpn, m_vpn_req)

    def _read_translation_page(self, m_vpn):
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        mapping_dict = self.mapping_on_flash.lpns_to_ppns(lpns)

        # as if we readlly read from flash
        m_ppn = self.directory.m_vpn_to_m_ppn(m_vpn)
        yield self.env.process(
                self.flash.rw_ppn_extent(m_ppn, 1, 'read', tag = None))

        self.env.exit(mapping_dict)

    def _write_back(self, m_vpn):
        """
        It should not call _load_to_free_space() directly or indirectly as it
        will deadlock.
        """
        # aquire lock
        m_vpn_req = self.vpn_res_pool.get_request(m_vpn)
        yield m_vpn_req # should take no time, usually

        mapping_in_cache = self._cached_mappings.get_m_vpn_mappings(m_vpn)

        # mark entries in cache as clean
        # we are sure that the mapping in flash will be the same as
        # the mappings in cache, so YES, it is clean now. You don't need
        # to wait until it is on the flash to say so. At that time
        # you don't know if cache and flash mapping is the same.
        self._cached_mappings.batch_overwrite_existing(mapping_in_cache,
                dirty = False)

        if len(mapping_in_cache) < self.conf.n_mapping_entries_per_page:
            # Not all mappings are in cache
            mapping_in_flash = yield self.env.process(
                    self._read_translation_page(m_vpn))
            latest_mapping = mapping_in_flash
            latest_mapping.update(mapping_in_cache)
        else:
            # all mappings are in cache, no need to read the translation page
            latest_mapping = mapping_in_cache

        yield self.env.process(
            self._update_mapping_on_flash(m_vpn, latest_mapping))

        # release lock
        self.vpn_res_pool.release_request(m_vpn, m_vpn_req)

    def _update_mapping_on_flash(self, m_vpn, mapping_dict):
        """
        mapping_dict should only has lpns belonging to m_vpn
        """
        # mapping_dict has to have all and only the entries of m_vpn
        lpn_sample = mapping_dict.keys()[0]
        tmp_m_vpn = self.conf.lpn_to_m_vpn(lpn_sample)
        assert tmp_m_vpn == m_vpn
        assert len(mapping_dict) == self.conf.n_mapping_entries_per_page

        self.mapping_on_flash.batch_update(mapping_dict)

        yield self.env.process(self._program_translation_page(m_vpn))

    def _program_translation_page(self, m_vpn):
        new_m_ppn = self.block_pool.next_translation_page_to_program()
        old_m_ppn = self.directory.m_vpn_to_m_ppn(m_vpn)

        yield self.env.process(
                self.flash.rw_ppn_extent(new_m_ppn, 1, 'write', tag = None))

        self.oob.new_write(lpn = m_vpn, old_ppn = old_m_ppn,
            new_ppn = new_m_ppn)
        self.directory.update_mapping(m_vpn = m_vpn, m_ppn = new_m_ppn)

        assert self.oob.states.is_page_valid(old_m_ppn) == False
        assert self.oob.states.is_page_valid(new_m_ppn) == True
        assert self.oob.ppn_to_lpn_mvpn[new_m_ppn] == m_vpn
        assert self.directory.m_vpn_to_m_ppn(m_vpn) == new_m_ppn



class MappingTable(object):
    """
    When do we need batched update?
    - do we need it when cleaning translation pages? NO. cleaning translation
    pages does not change contents of translation page.
    - do we need it when cleaning data page? Yes. When cleaning data page, you
    need to modify some lpn->ppn. For those LPNs in the same translation page,
    you can group them and update together. The process is: put those LPNs to
    the same group, read the translation page, modify entries and write it to
    flash. If you want batch updates here, you will need to buffer a few
    lpn->ppn. Well, since we have limited SRAM, you cannot do this.
    TODO: maybe you need to implement this.

    - do we need it when writing a lpn? To be exact, we need it when evict an
    entry in CMT. In that case, we need to find all the CMT entries in the same
    translation page with the victim entry.
    """
    def __init__(self, confobj):
        self.conf = confobj

        self.entry_bytes = self.conf['cache_entry_bytes'] # lpn + ppn
        max_bytes = self.conf.mapping_cache_bytes
        self.max_n_entries = (max_bytes + self.entry_bytes - 1) / \
            self.entry_bytes
        print 'cache max entries', self.max_n_entries, ',', \
            'for data of size', self.max_n_entries * self.conf.page_size, 'MB'
        if self.conf.n_mapping_entries_per_page > self.max_n_entries:
            raise ValueError("Because we may read a whole translation page "\
                    "cache. We need the cache to be large enough to hold "\
                    "it. So we don't lost any entries. But now "\
                    "n_mapping_entries_per_page {} is larger than "\
                    "max_n_entries {}.".format(
                    self.conf.n_mapping_entries_per_page,
                    self.max_n_entries))

        # self.entries = {}
        # self.entries = lrulist.LruCache()
        self.entries = lrulist.SegmentedLruCache(self.max_n_entries, 0.5)

    def lpn_to_ppn(self, lpn):
        "Try to find ppn of the given lpn in cache"
        entry_data = self.entries.get(lpn, MISS)
        if entry_data == MISS:
            return MISS
        else:
            return entry_data.ppn

    def batch_update(self, mapping_dict, dirty):
        for lpn, ppn in mapping_dict.items():
            self.update(lpn, ppn, dirty)

    def update(self, lpn, ppn, dirty):
        self.entries[lpn] = CacheEntryData(lpn = lpn, ppn = ppn, dirty = dirty)

    def add_new_entry(self, lpn, ppn, dirty):
        "dirty is a boolean"
        if self.entries.has_key(lpn):
            raise RuntimeError("{}:{} already exists in CMT entries.".format(
                lpn, self.entries[lpn].ppn))
        self.entries[lpn] = CacheEntryData(lpn = lpn, ppn = ppn, dirty = dirty)

    def add_new_entries_if_not_exist(self, mappings, dirty):
        """
        mappings {
            lpn1: ppn1
            lpn2: ppn2
            lpn3: ppn3
            }
        """
        for lpn, ppn in mappings.items():
            self.add_new_entry_if_not_exist(lpn, ppn, dirty)

    def add_new_entry_if_not_exist(self, lpn, ppn, dirty):
        """
        If lpn is already in cache, don't add it. Don't change dirty bit.
        If lpn is not in cache, add it and set dirty to False
        """
        if not self.entries.has_key(lpn):
            self.entries[lpn] = \
                CacheEntryData(lpn = lpn, ppn = ppn, dirty = dirty)

    def needed_space_for_m_vpn(self, m_vpn):
        cached_mappings = self.get_m_vpn_mappings(m_vpn)
        return self.conf.n_mapping_entries_per_page - len(cached_mappings)

    def get_m_vpn_mappings(self, m_vpn):
        """ return all the mappings of m_vpn that are in cache
        """
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        mapping_dict = MappingDict()
        for lpn in lpns:
            ppn = self.lpn_to_ppn(lpn)
            if ppn != MISS:
                mapping_dict[lpn] = ppn

        return mapping_dict

    def overwrite_entry(self, lpn, ppn, dirty):
        "lpn must exist"
        self.entries[lpn].ppn = ppn
        self.entries[lpn].dirty = dirty

    def batch_overwrite_existing(self, mapping_dict, dirty):
        for lpn, ppn in mapping_dict.items():
            self.overwrite_quietly(lpn, ppn, dirty)

    def overwrite_quietly(self, lpn, ppn, dirty):
        entry_data = self._peek(lpn)
        entry_data.ppn = ppn
        entry_data.dirty = dirty

    def remove_entry_by_lpn(self, lpn):
        assert not self._is_dirty(lpn)
        del self.entries[lpn]

    def victim_entry(self):
        # lpn = random.choice(self.entries.keys())
        classname = type(self.entries).__name__
        if classname in ('SegmentedLruCache', 'LruCache'):
            lpn = self.entries.victim_key()
        else:
            raise RuntimeError("You need to specify victim selection")

        # lpn, Cacheentrydata
        return lpn, self.entries.peek(lpn)

    def _peek(self, lpn):
        return self.entries.peek(lpn)

    def has_lpn(self, lpn):
        return self.entries.has_key(lpn)

    def delete_entries_of_m_vpn(self, m_vpn):
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        for lpn in lpns:
            try:
                self.remove_entry_by_lpn(lpn)
            except KeyError:
                # OK if it is not there
                pass

    def _is_dirty(self, lpn):
        return self._peek(lpn).dirty

    def is_m_vpn_dirty(self, m_vpn):
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        for lpn in lpns:
            if self._is_dirty(lpn):
                return True
        return False

    def is_full(self):
        return self.count() >= self.max_n_entries

    def is_overflowed(self):
        return self.count() > self.max_n_entries

    def count(self):
        return len(self.entries)

    def count_of_free(self):
        return self.max_n_entries - self.count()

    def __repr__(self):
        return repr(self.entries)

class CacheEntryData(object):
    """
    This is a helper class that store entry data for a LPN
    """
    def __init__(self, lpn, ppn, dirty):
        self.lpn = lpn
        self.ppn = ppn
        self.dirty = dirty

    def __repr__(self):
        return "lpn:{}, ppn:{}, dirty:{}".format(self.lpn,
            self.ppn, self.dirty)


class MappingOnFlash(object):
    """
    This mapping table is for data pages, not for translation pages.
    GMT should have entries as many as the number of pages in flash
    """
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not conf.Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj

        self.n_entries_per_page = self.conf.n_mapping_entries_per_page

        # do the easy thing first, if necessary, we can later use list or
        # other data structure
        self.entries = {}

    def lpn_to_ppn(self, lpn):
        """
        GMT should always be able to answer query. It is perfectly OK to return
        None because at the beginning there is no mapping. No valid data block
        on device.
        """
        return self.entries.get(lpn, UNINITIATED)

    def update(self, lpn, ppn):
        self.entries[lpn] = ppn

    def batch_update(self, mapping_dict):
        for lpn, ppn in mapping_dict.items():
            self.update(lpn, ppn)

    def lpns_to_ppns(self, lpns):
        d = MappingDict()
        for lpn in lpns:
            d[lpn] = self.lpn_to_ppn(lpn)

        return d

    def __repr__(self):
        return "global mapping table: {}".format(repr(self.entries))


class GlobalTranslationDirectory(object):
    """
    This is an in-memory data structure. It is only for book keeping. It used
    to remeber thing so that we don't lose it.
    """
    def __init__(self, confobj, oob, block_pool):
        self.conf = confobj

        self.flash_npage_per_block = self.conf.n_pages_per_block
        self.flash_num_blocks = self.conf.n_blocks_per_dev
        self.flash_page_size = self.conf.page_size
        self.total_pages = self.conf.total_num_pages()
        self.oob = oob
        self.block_pool = block_pool

        self.n_entries_per_page = self.conf.n_mapping_entries_per_page

        # M_VPN -> M_PPN
        # Virtual translation page number --> Physical translation page number
        # Dftl should initialize
        self.mapping = {}

        self._initialize()

    def _initialize(self):
        """
        This function initialize global translation directory. We assume the
        GTD is very small and stored in flash before mounting. We also assume
        that the global mapping table has been prepared by the vendor, so there
        is no other overhead except for reading the GTD from flash. Since the
        overhead is very small, we ignore it.
        """
        total_pages = self.conf.total_translation_pages()

        # use some free blocks to be translation blocks
        tmp_blk_mapping = {}
        for m_vpn in range(total_pages):
            m_ppn = self.block_pool.next_translation_page_to_program()
            # Note that we don't actually read or write flash
            self.add_mapping(m_vpn=m_vpn, m_ppn=m_ppn)
            # update oob of the translation page
            self.oob.new_write(lpn = m_vpn, old_ppn = UNINITIATED,
                new_ppn = m_ppn)

    def m_vpn_to_m_ppn(self, m_vpn):
        """
        m_vpn virtual translation page number. It should always be successfull.
        """
        return self.mapping[m_vpn]

    def add_mapping(self, m_vpn, m_ppn):
        if self.mapping.has_key(m_vpn):
            raise RuntimeError("self.mapping already has m_vpn:{}"\
                .format(m_vpn))
        self.mapping[m_vpn] = m_ppn

    def update_mapping(self, m_vpn, m_ppn):
        self.mapping[m_vpn] = m_ppn

    def remove_mapping(self, m_vpn):
        del self.mapping[m_vpn]

    def lpn_to_m_ppn(self, lpn):
        m_vpn = self.conf.lpn_to_m_vpn(lpn)
        m_ppn = self.m_vpn_to_m_ppn(m_vpn)
        return m_ppn

    def __repr__(self):
        return repr(self.mapping)


class GarbageCollector(object):
    def __init__(self, confobj, flashobj, oobobj, block_pool, mapping_manager,
        recorderobj, envobj):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj
        self.env = envobj

        self.mapping_manager = mapping_manager

        self.decider = GcDecider(self.conf, self.block_pool,
                self.recorder)

        self.victim_block_seqid = 0

    def gc_process(self):
        triggered = False

        self.decider.refresh()
        while self.decider.need_cleaning():
            if self.decider.call_index == 0:
                triggered = True
                self.recorder.count_me("GC", "invoked")
                print 'GC is triggerred', self.block_pool.used_ratio(), \
                    'usedblocks:', self.block_pool.total_used_blocks(), \
                    'data_usedblocks:', len(self.block_pool.data_usedblocks), \
                    'trans_usedblocks:', len(self.block_pool.trans_usedblocks), \
                    'freeblocks:', len(self.block_pool.freeblocks)
                block_iter = self.victim_blocks_iter()
                blk_cnt = 0
            try:
                blockinfo = block_iter.next()
            except StopIteration:
                print 'GC stoped from StopIteration exception'
                self.recorder.count_me("GC", "StopIteration")
                # high utilization, raise watermarkt to reduce GC attempts
                self.decider.raise_high_watermark()
                # nothing to be cleaned
                break
            victim_type, victim_block = (blockinfo.block_type,
                blockinfo.block_num)
            if victim_type == DATA_BLOCK:
                yield self.env.process(self.clean_data_block(victim_block))
            elif victim_type == TRANS_BLOCK:
                yield self.env.process(self.clean_trans_block(victim_block))
            blk_cnt += 1

        if triggered:
            print 'GC is finished', self.block_pool.used_ratio(), \
                blk_cnt, 'collected', \
                'freeblocks:', len(self.block_pool.freeblocks)
            # raise RuntimeError("intentional exit")

    def clean_data_block(self, flash_block):
        start, end = self.conf.block_to_page_range(flash_block)

        changes = []
        for ppn in range(start, end):
            if self.oob.states.is_page_valid(ppn):
                change = yield self.env.process(
                        self.move_data_page_to_new_location(ppn))
                changes.append(change)

        # change the mappings
        self.update_mapping_in_batch(changes)

        # mark block as free
        self.block_pool.move_used_data_block_to_free(flash_block)
        # it handles oob and flash
        yield self.env.process(
                self.erase_block(flash_block, DATA_CLEANING))

    def clean_trans_block(self, flash_block):
        yield self.env.process(
                self.move_valid_pages(flash_block,
                self.move_trans_page_to_new_location))
        # mark block as free
        self.block_pool.move_used_trans_block_to_free(flash_block)
        # it handles oob and flash
        yield self.env.process(
                self.erase_block(flash_block, TRANS_CLEAN))

    def move_valid_pages(self, flash_block, mover_func):
        start, end = self.conf.block_to_page_range(flash_block)

        for ppn in range(start, end):
            if self.oob.states.is_page_valid(ppn):
                yield self.env.process( mover_func(ppn) )

    def move_valid_data_pages(self, flash_block, mover_func):
        """
        With batch update:
        1. Move all valid pages to new location.
        2. Aggregate mappings in the same translation page and update together
        """
        start, end = self.conf.block_to_page_range(flash_block)

        for ppn in range(start, end):
            if self.oob.states.is_page_valid(ppn):
                mover_func(ppn)

    def move_data_page_to_new_location(self, ppn):
        """
        This function only moves data pages, but it does not update mappings.
        It will return the mappings changes to so another function can update
        the mapping.
        """
        # for my damaged brain
        old_ppn = ppn

        # read the the data page
        self.env.process(
                self.flash.rw_ppn_extent(old_ppn, 1, op = 'read',
                    tag = TAG_BACKGROUND))

        # find the mapping
        lpn = self.oob.translate_ppn_to_lpn(old_ppn)

        # write to new page
        new_ppn = self.block_pool.next_gc_data_page_to_program()
        self.env.process(
                self.flash.rw_ppn_extent(new_ppn, 1, op = 'write',
                    tag = TAG_BACKGROUND))

        # update new page and old page's OOB
        self.oob.data_page_move(lpn, old_ppn, new_ppn)

        ret = {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn}
        self.env.exit(ret)

    def group_changes(self, changes):
        """
        ret groups:
            { m_vpn_1: [
                      {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
                      {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
                      ...],
              m_vpn_2: [
                      {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
                      {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
                      ...],
        """
        # Put the mapping changes into groups, each group belongs to one mvpn
        groups = {}
        for change in changes:
            m_vpn = self.conf.lpn_to_m_vpn(change['lpn'])
            group = groups.setdefault(m_vpn, [])
            group.append(change)

        return groups

    def update_flash_mappings(self, m_vpn, changes_list):
        # update translation page on flash
        new_mappings = {change['lpn']:change['new_ppn']
                for change in changes_list}
        yield self.env.process(
                self.mapping_manager.update_translation_page_on_flash(
                m_vpn, new_mappings, TRANS_UPDATE_FOR_DATA_GC))

    def update_cache_mappings(self, changes_in_cache):
        # some mappings are in flash and some in cache
        # we can set mappings in cache as dirty=False since
        # they are consistent with flash
        for change in changes_in_cache:
            lpn = change['lpn']
            old_ppn = change['old_ppn']
            new_ppn = change['new_ppn']
            self.mapping_manager.mapping_table\
                .overwrite_entry(
                lpn = lpn, ppn = new_ppn, dirty = False)

    def apply_mvpn_changes(self, m_vpn, changes_list):
        """
        changes
          [
              {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
              {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
          ...]

        # if some mappings are in cache and some are in flash, you
        # can set dirty=False since both cache and flash will be
        # updated.
        # if all mappings are in cache you need to set dirty=True
        # since flash will not be updated
        # if all mappings are in flash you do nothing with cache
        """
        changes_in_cache = []
        some_in_cache = False
        some_in_flash = False
        for change in changes_list:
            lpn = change['lpn']
            old_ppn = change['old_ppn']
            new_ppn = change['new_ppn']

            cached_ppn = self.mapping_manager\
                .mapping_table.lpn_to_ppn(lpn)
            if cached_ppn != MISS:
                # lpn is in cache
                some_in_cache = True
                self.mapping_manager.mapping_table.overwrite_entry(
                    lpn = lpn, ppn = new_ppn, dirty = True)
                changes_in_cache.append(change)
            else:
                # lpn is not in cache, mark it and update later in batch
                some_in_flash = True

        if some_in_flash == True:
            yield self.env.process(
                    self.update_flash_mappings(m_vpn, changes_list))
            if some_in_cache == True:
                self.update_cache_mappings(changes_in_cache)

    def update_mapping_in_batch(self, changes):
        """
        changes is a table in the form of:
        [
          {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
          {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn},
          ...
        ]

        This function groups the LPNs that in the same MVPN and updates them
        together.

        If a MVPN has some entries in cache and some not, we need to update
        both cache (for the ones in cache) and the on-flash translation page.
        If a MVPN has only entries in cache, we will only update cache, and
            mark them dirty
        If a MVPN has only entries on flash, we will only update flash.
        """
        # Put the mapping changes into groups, each group belongs to one mvpn
        groups = self.group_changes(changes)

        for m_vpn, changes_list in groups.items():
            self.apply_mvpn_changes(m_vpn, changes_list)
    def move_trans_page_to_new_location(self, m_ppn):
        """
        1. read the trans page
        2. write to new location
        3. update OOB
        4. update GTD
        """
        old_m_ppn = m_ppn

        m_vpn = self.oob.translate_ppn_to_lpn(old_m_ppn)

        yield self.env.process(
            self.flash.rw_ppn_extent(old_m_ppn, 1, op = 'read',
                tag = TAG_BACKGROUND))

        # write to new page
        new_m_ppn = self.block_pool.next_gc_translation_page_to_program()
        yield self.env.process(
            self.flash.rw_ppn_extent(new_m_ppn, 1, op = 'write',
                tag = TAG_BACKGROUND))

        # update new page and old page's OOB
        self.oob.new_write(m_vpn, old_m_ppn, new_m_ppn)

        # update GTD
        self.mapping_manager.directory.update_mapping(m_vpn = m_vpn,
            m_ppn = new_m_ppn)

    def benefit_cost(self, blocknum, current_time):
        """
        This follows the DFTL paper
        """
        valid_ratio = self.oob.states.block_valid_ratio(blocknum)

        if valid_ratio == 0:
            # empty block is always the best deal
            return float("inf"), valid_ratio

        if valid_ratio == 1:
            # it is possible that none of the pages in the block has been
            # invalidated yet. In that case, all pages in the block is valid.
            # we don't need to clean it.
            return 0, valid_ratio

        last_inv_time = self.oob.last_inv_time_of_block.get(blocknum, None)
        if last_inv_time == None:
            print blocknum
            raise RuntimeError(
                "blocknum {} has never been invalidated."\
                "valid ratio:{}."
                .format(blocknum, valid_ratio))

        age = current_time - self.oob.last_inv_time_of_block[blocknum]
        age = age.total_seconds()
        bene_cost = age * ( 1 - valid_ratio ) / ( 2 * valid_ratio )

        return bene_cost, valid_ratio

    def victim_blocks_iter(self):
        """
        Calculate benefit/cost and put it to a priority queue
        """
        current_blocks = self.block_pool.current_blocks()
        current_time = datetime.datetime.now()
        priority_q = Queue.PriorityQueue()

        for usedblocks, block_type in (
            (self.block_pool.data_usedblocks, DATA_BLOCK),
            (self.block_pool.trans_usedblocks, TRANS_BLOCK)):
            for blocknum in usedblocks:
                if blocknum in current_blocks:
                    continue

                bene_cost, valid_ratio = self.benefit_cost(blocknum,
                    current_time)

                if bene_cost == 0:
                    # valid_ratio must be zero, we definitely don't
                    # want to cleaning it because we cannot get any
                    # free pages from it
                    continue

                blk_info = BlockInfo(block_type = block_type,
                    block_num = blocknum, value = bene_cost)
                blk_info.valid_ratio = valid_ratio

                if blk_info.valid_ratio > 0:
                    lpns = self.oob.lpns_of_block(blocknum)
                    s, e = self.conf.block_to_page_range(blocknum)
                    ppns = range(s, e)

                    ppn_states = [self.oob.states.page_state_human(ppn)
                        for ppn in ppns]
                    blk_info.mappings = zip(ppns, lpns, ppn_states)

                priority_q.put(blk_info)

        while not priority_q.empty():
            b_info =  priority_q.get()

            # record the information of victim block
            self.recorder.count_me('block.info.valid_ratio',
                round(b_info.valid_ratio, 2))
            self.recorder.count_me('block.info.bene_cost',
                round(b_info.value))

            if self.conf['record_bad_victim_block'] == True and \
                b_info.valid_ratio > 0:
                self.recorder.write_file('bad_victim_blocks',
                    block_type = b_info.block_type,
                    block_num = b_info.block_num,
                    bene_cost = b_info.value,
                    valid_ratio = round(b_info.valid_ratio, 2))

                # lpn ppn ppn_states blocknum
                for ppn, lpn, ppn_state in b_info.mappings:
                    if b_info.block_type == DATA_BLOCK:
                        lpn_timestamp = self.oob.timestamp_table[ppn]
                    else:
                        lpn_timestamp = -1

                    self.recorder.write_file('bad.block.mappings',
                        ppn = ppn,
                        lpn = lpn,
                        ppn_state = ppn_state,
                        block_num = b_info.block_num,
                        valid_ratio = b_info.valid_ratio,
                        block_type = b_info.block_type,
                        victim_block_seqid = self.victim_block_seqid,
                        lpn_timestamp = lpn_timestamp
                        )

            self.victim_block_seqid += 1

            yield b_info

    def erase_block(self, blocknum, tag):
        """
        THIS IS NOT A PUBLIC API
        set pages' oob states to ERASED
        electrionically erase the pages
        """
        # set page states to ERASED and in-OOB lpn to nothing
        self.oob.erase_block(blocknum)

        yield self.env.process(
            self.flash.erase_pbn_extent(blocknum, 1, tag = TAG_BACKGROUND))

class BlockInfo(object):
    """
    This is for sorting blocks to clean the victim.
    """
    def __init__(self, block_type, block_num, value):
        self.block_type = block_type
        self.block_num = block_num
        self.value = value

    def __comp__(self, other):
        "You can switch between benefit/cost and greedy"
        return cmp(self.valid_ratio, other.valid_ratio)
        # return cmp(self.value, other.value)

class GcDecider(object):
    """
    It is used to decide wheter we should do garbage collection.

    When need_cleaning() is called the first time, use high water mark
    to decide if we need GC.
    Later, use low water mark and progress to decide. If we haven't make
    progress in 10 times, stop GC
    """
    def __init__(self, confobj, block_pool, recorderobj):
        self.conf = confobj
        self.block_pool = block_pool
        self.recorder = recorderobj

        # Check if the high_watermark is appropriate
        # The high watermark should not be lower than the file system size
        # because if the file system is full you have to constantly GC and
        # cannot get more space
        min_high = 1 / float(self.conf.over_provisioning)
        if self.conf.GC_threshold_ratio < min_high:
            hi_watermark_ratio = min_high
            print 'High watermark is reset to {}. It was {}'.format(
                hi_watermark_ratio, self.conf.GC_threshold_ratio)
        else:
            hi_watermark_ratio = self.conf.GC_threshold_ratio
            print 'Using user defined high watermark', hi_watermark_ratio

        self.high_watermark = hi_watermark_ratio * \
            self.conf.n_blocks_per_dev

        spare_blocks = (1 - hi_watermark_ratio) * self.conf.n_blocks_per_dev
        if not spare_blocks > 32:
            raise RuntimeError("Num of spare blocks {} may not be enough"\
                "for garbage collection. You may encounter "\
                "Out Of Space error!".format(spare_blocks))

        min_low = 0.8 * 1 / self.conf.over_provisioning
        if self.conf.GC_low_threshold_ratio < min_low:
            low_watermark_ratio = min_low
            print 'Low watermark is reset to {}. It was {}'.format(
                low_watermark_ratio, self.conf.GC_low_threshold_ratio)
        else:
            low_watermark_ratio = self.conf.GC_low_threshold_ratio
            print 'Using user defined low watermark', low_watermark_ratio

        self.low_watermark = low_watermark_ratio * \
            self.conf.n_blocks_per_dev

        print 'High watermark', self.high_watermark
        print 'Low watermark', self.low_watermark

        self.call_index = -1
        self.last_used_blocks = None
        self.freeze_count = 0

    def refresh(self):
        """
        TODO: this class needs refactoring.
        """
        self.call_index = -1
        self.last_used_blocks = None
        self.freeze_count = 0

    def need_cleaning(self):
        "The logic is a little complicated"
        self.call_index += 1

        n_used_blocks = self.block_pool.total_used_blocks()

        if self.call_index == 0:
            # clean when above high_watermark
            ret = n_used_blocks > self.high_watermark
            # raise the high water mark because we want to avoid frequent GC
            if ret == True:
                self.raise_high_watermark()
        else:
            if self.freezed_too_long(n_used_blocks):
                ret = False
                print 'freezed too long, stop GC'
                self.recorder.count_me("GC", 'freezed_too_long')
            else:
                # Is it higher than low watermark?
                ret = n_used_blocks > self.low_watermark
                if ret == False:
                    self.recorder.count_me("GC", 'below_lowerwatermark')
                    # We were able to bring used block to below lower
                    # watermark. It means we still have a lot free space
                    # We don't need to worry about frequent GC.
                    self.reset_high_watermark()

        return ret

    def reset_high_watermark(self):
        return

        self.high_watermark = self.high_watermark_orig

    def raise_high_watermark(self):
        """
        Raise high watermark.

        95% of the total blocks are the highest possible
        """
        return

        self.high_watermark = min(self.high_watermark * 1.01,
            self.conf.n_blocks_per_dev * 0.95)

    def lower_high_watermark(self):
        """
        THe lowest is the original value
        """
        return

        self.high_watermark = max(self.high_watermark_orig,
            self.high_watermark / 1.01)

    def improved(self, cur_n_used_blocks):
        """
        wether we get some free blocks since last call of this function
        """
        if self.last_used_blocks == None:
            ret = True
        else:
            # common case
            ret = cur_n_used_blocks < self.last_used_blocks

        self.last_used_blocks = cur_n_used_blocks
        return ret

    def freezed_too_long(self, cur_n_used_blocks):
        if self.improved(cur_n_used_blocks):
            self.freeze_count = 0
            ret = False
        else:
            self.freeze_count += 1

            if self.freeze_count > 2 * self.conf.n_pages_per_block:
                ret = True
            else:
                ret = False

        return ret


class OutOfBandAreas(object):
    """
    It is used to hold page state and logical page number of a page.
    It is not necessary to implement it as list. But the interface should
    appear to be so.  It consists of page state (bitmap) and logical page
    number (dict).  Let's proivde more intuitive interfaces: OOB should accept
    events, and react accordingly to this event. The action may involve state
    and lpn_of_phy_page.
    """
    def __init__(self, confobj):
        self.conf = confobj

        self.flash_num_blocks = confobj.n_blocks_per_dev
        self.flash_npage_per_block = confobj.n_pages_per_block
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        # Key data structures
        self.states = ftlbuilder.FlashBitmap2(confobj)
        # ppn->lpn mapping stored in OOB, Note that for translation pages, this
        # mapping is ppn -> m_vpn
        self.ppn_to_lpn_mvpn = {}
        # Timestamp table PPN -> timestamp
        # Here are the rules:
        # 1. only programming a PPN updates the timestamp of PPN
        #    if the content is new from FS, timestamp is the timestamp of the
        #    LPN
        #    if the content is copied from other flash block, timestamp is the
        #    same as the previous ppn
        # 2. discarding, and reading a ppn does not change it.
        # 3. erasing a block will remove all the timestamps of the block
        # 4. so cur_timestamp can only be advanced by LBA operations
        self.timestamp_table = {}
        self.cur_timestamp = 0

        # flash block -> last invalidation time
        # int -> timedate.timedate
        self.last_inv_time_of_block = {}

    ############# Time stamp related ############
    def timestamp(self):
        """
        This function will advance timestamp
        """
        t = self.cur_timestamp
        self.cur_timestamp += 1
        return t

    def timestamp_set_ppn(self, ppn):
        self.timestamp_table[ppn] = self.timestamp()

    def timestamp_copy(self, src_ppn, dst_ppn):
        self.timestamp_table[dst_ppn] = self.timestamp_table[src_ppn]

    def translate_ppn_to_lpn(self, ppn):
        return self.ppn_to_lpn_mvpn[ppn]

    def wipe_ppn(self, ppn):
        self.states.invalidate_page(ppn)
        block, _ = self.conf.page_to_block_off(ppn)
        self.last_inv_time_of_block[block] = datetime.datetime.now()

        # It is OK to delay it until we erase the block
        # try:
            # del self.ppn_to_lpn_mvpn[ppn]
        # except KeyError:
            # # it is OK that the key does not exist, for example,
            # # when discarding without writing to it
            # pass

    def wipe_ppns(self, ppns):
        for ppn in ppns:
            self.wipe_ppn(ppn)

    def erase_block(self, flash_block):
        self.states.erase_block(flash_block)

        start, end = self.conf.block_to_page_range(flash_block)
        for ppn in range(start, end):
            try:
                del self.ppn_to_lpn_mvpn[ppn]
                # if you try to erase translation block here, it may fail,
                # but it is expected.
                del self.timestamp_table[ppn]
            except KeyError:
                pass

        del self.last_inv_time_of_block[flash_block]

    def new_write(self, lpn, old_ppn, new_ppn):
        """
        mark the new_ppn as valid
        update the LPN in new page's OOB to lpn
        invalidate the old_ppn, so cleaner can GC it
        """
        self.states.validate_page(new_ppn)
        self.ppn_to_lpn_mvpn[new_ppn] = lpn

        if old_ppn != UNINITIATED:
            # the lpn has mapping before this write
            self.wipe_ppn(old_ppn)

    def new_lba_write(self, lpn, old_ppn, new_ppn):
        """
        This is exclusively for lba_write(), so far
        """
        self.timestamp_set_ppn(new_ppn)
        self.new_write(lpn, old_ppn, new_ppn)

    def data_page_move(self, lpn, old_ppn, new_ppn):
        # move data page does not change the content's timestamp, so
        # we copy
        self.timestamp_copy(src_ppn = old_ppn, dst_ppn = new_ppn)
        self.new_write(lpn, old_ppn, new_ppn)

    def lpns_of_block(self, flash_block):
        s, e = self.conf.block_to_page_range(flash_block)
        lpns = []
        for ppn in range(s, e):
            lpns.append(self.ppn_to_lpn_mvpn.get(ppn, 'NA'))

        return lpns

def dec_debug(function):
    def wrapper(self, lpn):
        ret = function(self, lpn)
        if lpn == 38356:
            print function.__name__, 'lpn:', lpn, 'ret:', ret
        return ret
    return wrapper

class Config(config.ConfigNCQFTL):
    def __init__(self, confdic = None):
        super(Config, self).__init__(confdic)

        local_itmes = {
            # number of bytes per entry in mapping_on_flash
            "translation_page_entry_bytes": 4, # 32 bits
            "cache_entry_bytes": 8, # 4 bytes for lpn, 4 bytes for ppn
            "GC_threshold_ratio": 0.95,
            "GC_low_threshold_ratio": 0.9,
            "over_provisioning": 1.28,
            "mapping_cache_bytes": None # cmt: cached mapping table
            }
        self.update(local_itmes)

        # self['keeping_all_tp_entries'] = False
        self['keeping_all_tp_entries'] = True

    @property
    def keeping_all_tp_entries(self):
        return self['keeping_all_tp_entries']

    @keeping_all_tp_entries.setter
    def keeping_all_tp_entries(self, value):
        self['keeping_all_tp_entries'] = value

    @property
    def n_mapping_entries_per_page(self):
        return self.page_size / self['translation_page_entry_bytes']

    @property
    def mapping_cache_bytes(self):
        return self['mapping_cache_bytes']

    @mapping_cache_bytes.setter
    def mapping_cache_bytes(self, value):
        self['mapping_cache_bytes'] = value

    @property
    def n_cache_entries(self):
        return self.mapping_cache_bytes / self['cache_entry_bytes']

    @n_cache_entries.setter
    def n_cache_entries(self, value):
        self.mapping_cache_bytes = value * self['cache_entry_bytes']

    @property
    def translation_page_entry_bytes(self):
        return self['translation_page_entry_bytes']

    @property
    def over_provisioning(self):
        return self['over_provisioning']

    @property
    def GC_threshold_ratio(self):
        return self['GC_threshold_ratio']

    @property
    def GC_low_threshold_ratio(self):
        return self['GC_low_threshold_ratio']

    def sec_ext_to_page_ext(self, sector, count):
        """
        The sector extent has to be aligned with page
        return page_start, page_count
        """
        page = sector / self.n_secs_per_page
        page_end = (sector + count) / self.n_secs_per_page
        page_count = page_end - page
        if (sector + count) % self.n_secs_per_page != 0:
            page_count += 1
        return page, page_count

    def lpn_to_m_vpn(self, lpn):
        return lpn / self.n_mapping_entries_per_page

    def m_vpn_to_lpns(self, m_vpn):
        start_lpn = m_vpn * self.n_mapping_entries_per_page
        return range(start_lpn, start_lpn + self.n_mapping_entries_per_page)

    def total_translation_pages(self):
        """
        total number of translation pages needed. It is:
        total_entries * entry size / page size
        """
        n_entries = self.total_num_pages()
        entry_bytes = self.translation_page_entry_bytes
        flash_page_size = self.page_size
        # play the ceiling trick
        return (n_entries * entry_bytes + \
                (flash_page_size -1)) / flash_page_size


