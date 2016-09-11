import bitarray
from collections import deque, Counter
import csv
import datetime
import random
import os
import Queue
import sys

import bidict

import config
import flash
import ftlbuilder
import lrulist
import recorder
from utilities import utils
from .blkpool import BlockPool
from .bitmap import FlashBitmap2

"""
This refactors Dftl

Notes for DFTL design

Components
- block pool: it should have free list, used data blocks and used translation
  blocks. We should be able to find out the next free block from here. The DFTL
  paper does not mention a in-RAM data structure like this. How do they find
  out the next free block?
    - it manages the appending point for different purpose. No, it does not
      have enough info to do that.

- Cached Mapping Table (CMT): this class should do the following:
    - have logical page <-> physical page mapping entries.
    - be able to translation LPN to PPN and PPN to LPN
    - implement a replacement policy
    - has a size() method to output the size of the CMT, so we know it does not
      exceed the size of SRAM
    - be able to add one entry
    - be able to remove one entry
    - be able to find the proper entry to be moved.
    - be able to set the number of entries allows by size
    - be able to set the number of entries directly
    - be able to fetch a mapping entry (this need to consult the Global
      Translation Directory)
    - be able to evict one entrie to flash
    - be able to evict entries to flash in batch

    - CMT interacts with
        - Flash: to save and read translation pages
        - block pool: to find free block to evict translation pages
        - Global Translation Directory: to find out where the translation pages
          are (so you can read/write), also, you need to update GTD when
          evicting pages to flash.
        - bitmap???

- Global mapping table (GMT): this class holds the mappings of all the data
  pages. In real implementation, this should be stored in flash. This data
  structure will be used intensively. It should have a good interface.
    - API, get_entries_of_Mvpn(virtual mapping page number). This is one of the
      cases that it may be used: when reading a data page, its translation
      entry is not in CMT. The translator will consult the GTD to find the
      physical translation page number of virtual translation page number V.
      Then we need to get the content of V by reading its corresponding
      physical page. We may provide an interface to the translator like
      load_translation_physical_page(PPN), which internally, we read from GMT.

- Out of Band Area (OOB) of a page: it can hold:
    - page state (Invalid, Valid, Erased)
    - logical page number
    - ECC

- Global Translation Directory, this should do the following:
    - maintains the locations of translation pages
    - given a Virtual Translation Page Number, find out the physical
      translation page number
    - given a Logical Data Page Number, find out the physical data page number

    - GTD should be a pretty passive class, it interacts with
        - CMT. When CMT changes the location of translation pages, GTD should
          be updated to reflect the changes

- Garbage Collector
    - clean data blocks: to not interrupt current writing of pages, garbage
      collector should have its own appending point to write the garbage
      collected data
    - clean translation blocks. This cleaning should also have its own
      appending point.
    - NOTE: the DFTL paper says DFTL also have partial merge and switch merge,
      I need to read their code to find out why.
    - NOTE2: When cleaning a victim block, we need to know the corresponding
      logical page number of a vadlid physical page in the block. However, the
      Global Mapping Table does not provide physical to logical mapping. We can
      maintain such an mapping table by our own and assume that information is
      stored in OOB.

    - How the garbage collector should interact with other components?
        - this cleaner will use free blocks and make used block free, so it
          will need to interact with block pool to move blocks between
          different lists.
        - the cleaner also need to interact with CMT because it may move
          translation pages around
        - the cleaner also need to interact with bitmap because it needs to
          find out the if a page is valid or not.  It also need to find out the
          invalid ratio of the blocks
        - the cleaner also need to update the global translation directory
          since it moved pages.
        - NOTE: all the classes above should a provide an easy interface for
          the cleaner to use, so the cleaner does not need to use the low-level
          interfaces to implement these functions

- Appending points: there should be several appending points:
    - appending point for writing translation page
    - appending point for writing data page
    - appending ponit for garbage collection (NO, the paper says there is no
      such a appending point
    - NOTE: these points should be maintained by block pool.

- FLASH



********* Victim block selection ****************
Pick the block with the largest benefit/cost

benefit/cost = age * (1-u) / 2u

where u is the utilization of the segment and age is the
time since the most recent modification (i.e., the last
block invalidation). The terms 2u and 1-u respectively
represent the cost for copying (u to read valid blocks in
the segment and u to write back them) and the free
space reclaimed.

What's age?
The longer the age, the more benefit it has.
Since ?
    - the time the block was erased
    - the first time a page become valid
    - the last time a page become valid
        - if use this and age is long, .... it does not say anything about
        overwriting
    - the last time a page become invalid in the block
        - if use this and age is long, the rest of the valid pages are cold
        (long time no overwrite)


********* Profiling result ****************
   119377    0.628    0.000    0.829    0.000 ftlbuilder.py:100(validate_page)
    93220    0.427    0.000    0.590    0.000 ftlbuilder.py:104(invalidate_page)
  6507243   94.481    0.000  266.729    0.000 ftlbuilder.py:131(block_valid_ratio)
 26133103  105.411    0.000  147.733    0.000 ftlbuilder.py:141(is_page_valid)
    10963    0.073    0.000    0.084    0.000 ftlbuilder.py:145(is_page_invalid)
        1    0.000    0.000    0.000    0.000 ftlbuilder.py:187(__init__)
        2    0.000    0.000    0.000    0.000 ftlbuilder.py:75(__init__)
 26356663   42.697    0.000   42.697    0.000 ftlbuilder.py:86(pagenum_to_slice_range)
  6507243   29.636    0.000  296.394    0.000 dftl.py:1072(benefit_cost)
    24285   16.029    0.001  314.736    0.013 dftl.py:1125(victim_blocks_iter)
  6560378   12.825    0.000   12.825    0.000 config.py:53(block_to_page_range)


************** SRAM size ******************
In the DFTL paper, they say the minimum SRAM size is the size that is
required for hybrid FTL to work. In hybrid ftl, they use 3% of the flash
as log blocks. That means we need to keep the mapping for these 3% in SRAM.
For a 256MB flash, the number of pages we need to keep mapping for is
> (256*2^20/4096)*0.03
[1] 1966.08
about 2000 pages


*************** Batch update ***************
When evicting one mapping, do the following
- find all dirty mappings in the same translation page
- write all dirty mappings in the same translation page to flash
- mark all dirty mappings as clean
- delete the one mapping


*************** Trace back bad writes ******
This is to trace back where a LPN has been.

1. Number each write by the order that they enter the FTL, save this number to
    for use later. We call this number timestamp. We can keep a table of
    ppn->timestamp. Using ppn to find timestamp is because we want to find the
    original write that goes to a PPN, even when the LPN has been overwritten
    or discarded.
2. During GC, recorder the following information of the victim block:
    victim block number | PPN | LPN | timestamp | Validity during GC |.

    Timestamp can be easilty obtained from TimestampTble.
"""

UNINITIATED, MISS = ('UNINIT', 'MISS')
DATA_BLOCK, TRANS_BLOCK = ('data_block', 'trans_block')
random.seed(0)

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
    def cache_mapped_data_bytes(self):
        return self.n_cache_entries * self.page_size

    @cache_mapped_data_bytes.setter
    def cache_mapped_data_bytes(self, data_bytes):
        self.n_cache_entries = data_bytes / self.page_size
        if self.n_cache_entries % self.n_mapping_entries_per_page != 0:
            print "WARNING: size of mapping cache is not aligned with "\
                "translation page size."
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


class GlobalHelper(object):
    """
    In case you need some global variables. We put all global stuff here so
    it is easier to manage. (And you know all the bad things you did :)
    """
    def __init__(self, confobj):
        self.timeline = Timeline(confobj)

LOGICAL_READ, LOGICAL_WRITE, LOGICAL_DISCARD = ('LOGICAL_READ', \
        'LOGICAL_WRITE', 'LOGICAL_DISCARD')


class Timeline(object):
    """
    This is intended for global use. It maintains a table like:

    pid lpn operation    flash.read flash.write flash.erasure
    133 34  write        33         1           35
    555 77  read         3          5           666

    flash.* is the flash operations taken to finish the lpn operation

    This is how it should be used:
    1. the interface calls lba_read(), lba_write(), and lba_discard() will
    add a new row to the table,
    2. the flash class will simply increment the counter
    3. go to 1.

    """
    def __init__(self, confobj):
        self.table = []
        self.conf = confobj
        self.ON = False
        self.timestamp = 0

        self.name_map = {
            'flash.read':     'page_read_time',     # milli sec
            'flash.write':    'page_prog_time',     # milli sec
            'flash.erasure':  'block_erase_time'    # milli sec
        }

    def turn_on(self):
        self.ON = True

    def turn_off(self):
        self.ON = False

    def add_logical_op(self, sector, count, op):
        if not self.ON:
            return

        self.table.append( {'sector':sector, 'count':count, 'operation': op,
            'start_timestamp': self.timestamp,
            'end_timestamp': self.timestamp}
            )

    def incr_time_stamp(self, op, count):
        """
        Update clock and the end_timestamp of last sector operation
        """
        if not self.ON:
            return

        opname = self.name_map[op]
        time = self.conf['flash_config'][opname] * count
        self.timestamp += time

        last_row = self.table[-1]
        last_row['end_timestamp'] = self.timestamp

    def save(self):
        path = os.path.join(self.conf['result_dir'], 'timeline.txt')
        utils.table_to_file(self.table, path)


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
        self.states = FlashBitmap2(confobj)
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


class CachedMappingTable(object):
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

        self.entry_bytes = 8 # lpn + ppn
        max_bytes = self.conf.mapping_cache_bytes
        self.max_n_entries = (max_bytes + self.entry_bytes - 1) / \
            self.entry_bytes
        print 'cache max entries', self.max_n_entries, \
            self.max_n_entries * 4096 / 2**20, 'MB'

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

    def add_new_entry(self, lpn, ppn, dirty):
        "dirty is a boolean"
        if self.entries.has_key(lpn):
            raise RuntimeError("{}:{} already exists in CMT entries.".format(
                lpn, self.entries[lpn].ppn))
        self.entries[lpn] = CacheEntryData(lpn = lpn, ppn = ppn, dirty = dirty)

    def update_entry(self, lpn, ppn, dirty):
        "You may end up remove the old one"
        self.entries[lpn] = CacheEntryData(lpn = lpn, ppn = ppn, dirty = dirty)

    def overwrite_entry(self, lpn, ppn, dirty):
        "lpn must exist"
        self.entries[lpn].ppn = ppn
        self.entries[lpn].dirty = dirty

    def remove_entry_by_lpn(self, lpn):
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

    def is_full(self):
        n = len(self.entries)
        assert n <= self.max_n_entries
        return n == self.max_n_entries

    def __repr__(self):
        return repr(self.entries)


class MappingOnFlash(object):
    """
    This mapping table is for data pages, not for translation pages.
    GMT should have entries as many as the number of pages in flash
    """
    def __init__(self, confobj, flashobj):
        """
        flashobj is the flash device that we may operate on.
        """
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not conf.Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj

        self.n_entries_per_page = self.conf.n_mapping_entries_per_page

        # do the easy thing first, if necessary, we can later use list or
        # other data structure
        self.entries = {}

    def total_entries(self):
        """
        total number of entries stored in global mapping table.  It is the same
        as the number of pages in flash, since we use page-leveling mapping
        """
        return self.conf.total_num_pages()

    def total_translation_pages(self):
        """
        total number of translation pages needed. It is:
        total_entries * entry size / page size
        """
        entries = self.total_entries()
        entry_bytes = self.conf.translation_page_entry_bytes
        flash_page_size = self.conf.page_size
        # play the ceiling trick
        return (entries * entry_bytes + (flash_page_size -1))/flash_page_size

    def lpn_to_ppn(self, lpn):
        """
        GMT should always be able to answer query. It is perfectly OK to return
        None because at the beginning there is no mapping. No valid data block
        on device.
        """
        return self.entries.get(lpn, UNINITIATED)

    def update(self, lpn, ppn):
        self.entries[lpn] = ppn

    def __repr__(self):
        return "global mapping table: {}".format(repr(self.entries))


class GlobalTranslationDirectory(object):
    """
    This is an in-memory data structure. It is only for book keeping. It used
    to remeber thing so that we don't lose it.
    """
    def __init__(self, confobj):
        self.conf = confobj

        self.flash_npage_per_block = self.conf.n_pages_per_block
        self.flash_num_blocks = self.conf.n_blocks_per_dev
        self.flash_page_size = self.conf.page_size
        self.total_pages = self.conf.total_num_pages()

        self.n_entries_per_page = self.conf.n_mapping_entries_per_page

        # M_VPN -> M_PPN
        # Virtual translation page number --> Physical translation page number
        # Dftl should initialize
        self.mapping = {}

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

    def m_vpn_of_lpn(self, lpn):
        "Find the virtual translation page that holds lpn"
        return lpn / self.n_entries_per_page

    def m_vpn_to_lpns(self, m_vpn):
        start_lpn = m_vpn * self.n_entries_per_page
        return range(start_lpn, start_lpn + self.n_entries_per_page)

    def m_ppn_of_lpn(self, lpn):
        m_vpn = self.m_vpn_of_lpn(lpn)
        m_ppn = self.m_vpn_to_m_ppn(m_vpn)
        return m_ppn

    def __repr__(self):
        return repr(self.mapping)


class MappingManager(object):
    """
    This class is the supervisor of all the mappings. When initializing, it
    register CMT and GMT with it and provides higher level operations on top of
    them.
    This class should act as a coordinator of all the mapping data structures.
    """
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        self.conf = confobj

        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj

        # managed and owned by Mappingmanager
        self.mapping_on_flash = MappingOnFlash(confobj, flashobj)
        self.cached_mapping_table = CachedMappingTable(confobj)
        self.directory = GlobalTranslationDirectory(confobj)

    def ppns_for_writing(self, lpns):
        """
        This function returns ppns that can be written.

        The ppns returned are mapped by lpns, one to one
        """
        ppns = []
        for lpn in lpns:
            old_ppn = self.lpn_to_ppn(lpn)
            new_ppn = self.block_pool.next_data_page_to_program()
            ppns.append(new_ppn)
            # CMT
            # lpn must be in cache thanks to self.mapping_manager.lpn_to_ppn()
            self.cached_mapping_table.overwrite_entry(
                lpn = lpn, ppn = new_ppn, dirty = True)
            # OOB
            self.oob.new_lba_write(lpn = lpn, old_ppn = old_ppn,
                new_ppn = new_ppn)

        return ppns

    def ppns_for_reading(self, lpns):
        """
        """
        ppns = []
        for lpn in lpns:
            ppn = self.lpn_to_ppn(lpn)
            # request lpn must match oob[ppn].lpn
            if ppn != 'UNINIT':
                # assert_flash_data_startswith_oob_lpn(self.conf, self.flash,
                        # self.oob, ppn)
                check_data(self, self.conf, self.flash, self.oob, ppn,
                        lpn)
            ppns.append(ppn)

        return ppns

    def lpn_to_ppn(self, lpn):
        """
        This method does not fail. It will try everything to find the ppn of
        the given lpn.
        return: real PPN or UNINITIATED
        """
        # try cached mapping table first.
        ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        if ppn == MISS:
            # cache miss
            while self.cached_mapping_table.is_full():
                self.evict_cache_entry()

            # find the physical translation page holding lpn's mapping in GTD
            ppn = self.load_mapping_entry_to_cache(lpn)

            self.recorder.count_me("cache", "miss")
        else:
            self.recorder.count_me("cache", "hit")

        return ppn

    def load_mapping_entry_to_cache(self, lpn):
        """
        When a mapping entry is not in cache, you need to read the entry from
        flash and put it to cache. This function does this.
        Output: it return the ppn of lpn read from entry on flash.
        """
        # find the location of the translation page
        m_ppn = self.directory.m_ppn_of_lpn(lpn)

        # read it up, this operation is just for statistics
        self.flash.read_pages(ppns = [m_ppn], tag = TRANS_CACHE)

        # Now we have all the entries of m_ppn in memory, we need to put
        # the mapping of lpn->ppn to CMT
        ppn = self.mapping_on_flash.lpn_to_ppn(lpn)
        self.cached_mapping_table.add_new_entry(lpn = lpn, ppn = ppn,
            dirty = False)

        return ppn

    def initialize_mappings(self):
        """
        This function initialize global translation directory. We assume the
        GTD is very small and stored in flash before mounting. We also assume
        that the global mapping table has been prepared by the vendor, so there
        is no other overhead except for reading the GTD from flash. Since the
        overhead is very small, we ignore it.
        """
        total_pages = self.mapping_on_flash.total_translation_pages()

        # use some free blocks to be translation blocks
        tmp_blk_mapping = {}
        for m_vpn in range(total_pages):
            m_ppn = self.block_pool.next_translation_page_to_program()
            # Note that we don't actually read or write flash
            self.directory.add_mapping(m_vpn=m_vpn, m_ppn=m_ppn)
            # update oob of the translation page
            self.oob.new_write(lpn = m_vpn, old_ppn = UNINITIATED,
                new_ppn = m_ppn)

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
        cached_ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        if cached_ppn != MISS:
            # in cache
            self.cached_mapping_table.overwrite_entry(lpn = lpn,
                ppn = new_ppn, dirty = False)

        m_vpn = self.directory.m_vpn_of_lpn(lpn)

        # batch_entries may be empty
        batch_entries = self.dirty_entries_of_translation_page(m_vpn)

        new_mappings = {lpn:new_ppn} # lpn->new_ppn may not be in cache
        for entry in batch_entries:
            new_mappings[entry.lpn] = entry.ppn

        # update translation page
        self.update_translation_page_on_flash(m_vpn, new_mappings, tag)

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

        vic_lpn, vic_entrydata = self.cached_mapping_table.victim_entry()

        if vic_entrydata.dirty == True:
            # If we have to write to flash, we write in batch
            m_vpn = self.directory.m_vpn_of_lpn(vic_lpn)
            self.batch_write_back(m_vpn)

        # remove only the victim entry
        self.cached_mapping_table.remove_entry_by_lpn(vic_lpn)

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
        self.update_translation_page_on_flash(m_vpn, new_mappings, TRANS_CACHE)

        # mark them as clean
        for entry in batch_entries:
            entry.dirty = False

    def dirty_entries_of_translation_page(self, m_vpn):
        """
        Get all dirty entries in translation page m_vpn.
        """
        retlist = []
        for entry_lpn, dataentry in self.cached_mapping_table.entries.items():
            if dataentry.dirty == True:
                tmp_m_vpn = self.directory.m_vpn_of_lpn(entry_lpn)
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
            self.flash.read_pages(ppns = [old_m_ppn], tag = tag)
        else:
            self.recorder.count_me('cache', 'saved.1.read')

        pass # modify in memory. Since we are a simulator, we don't do anything
        new_m_ppn = self.block_pool.next_translation_page_to_program()

        # update flash
        self.flash.write_pages(ppns = [new_m_ppn], ppn_data = None, tag = tag)
        # update our fake 'on-flash' GMT
        for lpn, new_ppn in new_mappings.items():
            self.mapping_on_flash.update(lpn = lpn, ppn = new_ppn)

        # OOB, keep m_vpn as lpn
        self.oob.new_write(lpn = m_vpn, old_ppn = old_m_ppn,
            new_ppn = new_m_ppn)

        # update GTD so we can find it
        self.directory.update_mapping(m_vpn = m_vpn, m_ppn = new_m_ppn)


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


class GarbageCollector(object):
    def __init__(self, confobj, flashobj, oobobj, block_pool, mapping_manager,
        recorderobj):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj

        self.mapping_manager = mapping_manager

        self.decider = GcDecider(self.conf, self.block_pool, self.recorder)

        self.victim_block_seqid = 0

    def try_gc(self):
        triggered = False

        self.decider.refresh()
        while self.decider.need_cleaning():
            if self.decider.call_index == 0:
                triggered = True
                self.recorder.count_me("GC", "invoked")
                print 'GC is triggerred', self.block_pool.used_ratio(), \
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
                self.clean_data_block(victim_block)
            elif victim_type == TRANS_BLOCK:
                self.clean_trans_block(victim_block)
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
                change = self.move_data_page_to_new_location(ppn)
                changes.append(change)

        # change the mappings
        self.update_mapping_in_batch(changes)

        # mark block as free
        self.block_pool.move_used_data_block_to_free(flash_block)
        # it handles oob and flash
        self.erase_block(flash_block, DATA_CLEANING)

    def clean_trans_block(self, flash_block):
        self.move_valid_pages(flash_block,
            self.move_trans_page_to_new_location)
        # mark block as free
        self.block_pool.move_used_trans_block_to_free(flash_block)
        # it handles oob and flash
        self.erase_block(flash_block, TRANS_CLEAN)

    def move_valid_pages(self, flash_block, mover_func):
        start, end = self.conf.block_to_page_range(flash_block)

        for ppn in range(start, end):
            if self.oob.states.is_page_valid(ppn):
                mover_func(ppn)

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
        pagedata = self.flash.read_pages(ppns = [old_ppn],
                tag = DATA_CLEANING)[0]

        # find the mapping
        lpn = self.oob.translate_ppn_to_lpn(old_ppn)

        # write to new page
        new_ppn = self.block_pool.next_gc_data_page_to_program()
        self.flash.write_pages(ppns = [new_ppn], ppn_data = [pagedata],
                tag = DATA_CLEANING)

        # update new page and old page's OOB
        self.oob.data_page_move(lpn, old_ppn, new_ppn)

        if lpn == 6127:
            print 'we will move 6127'
            exit(1)

        return {'lpn':lpn, 'old_ppn':old_ppn, 'new_ppn':new_ppn}

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
            m_vpn = self.mapping_manager.directory.m_vpn_of_lpn(change['lpn'])
            group = groups.setdefault(m_vpn, [])
            group.append(change)

        return groups

    def update_flash_mappings(self, m_vpn, changes_list):
        # update translation page on flash
        new_mappings = {change['lpn']:change['new_ppn']
                for change in changes_list}
        self.mapping_manager.update_translation_page_on_flash(
                m_vpn, new_mappings, TRANS_UPDATE_FOR_DATA_GC)

    def update_cache_mappings(self, changes_in_cache):
        # some mappings are in flash and some in cache
        # we can set mappings in cache as dirty=False since
        # they are consistent with flash
        for change in changes_in_cache:
            lpn = change['lpn']
            old_ppn = change['old_ppn']
            new_ppn = change['new_ppn']
            self.mapping_manager.cached_mapping_table\
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
                .cached_mapping_table.lpn_to_ppn(lpn)
            if cached_ppn != MISS:
                # lpn is in cache
                some_in_cache = True
                self.mapping_manager.cached_mapping_table.overwrite_entry(
                    lpn = lpn, ppn = new_ppn, dirty = True)
                changes_in_cache.append(change)
            else:
                # lpn is not in cache, mark it and update later in batch
                some_in_flash = True

        if some_in_flash == True:
            self.update_flash_mappings(m_vpn, changes_list)
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

        self.flash.read_pages(ppns = [old_m_ppn], tag = TRANS_CLEAN)

        # write to new page
        new_m_ppn = self.block_pool.next_gc_translation_page_to_program()
        self.flash.write_pages(ppns = [new_m_ppn], ppn_data = None,
                tag = TRANS_CLEAN)

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

        self.flash.erase_blocks(pbns = [blocknum], tag = tag)

def dec_debug(function):
    def wrapper(self, lpn):
        ret = function(self, lpn)
        if lpn == 38356:
            print function.__name__, 'lpn:', lpn, 'ret:', ret
        return ret
    return wrapper

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

class Dftl(ftlbuilder.FtlBuilder):
    """
    The implementation literally follows DFtl paper.
    This class is a coordinator of other coordinators and data structures
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(Dftl, self).__init__(confobj, recorderobj, flashobj)

        if not isinstance(confobj, Config):
            raise TypeError("confobj is not Config. it is {}".
               format(type(confobj).__name__))

        # bitmap has been created parent class
        # Change: we now don't put the bitmap here
        # self.bitmap.initialize()
        # del self.bitmap

        self.global_helper = GlobalHelper(confobj)

        # Replace the flash object with a new one, which has global helper
        self.flash = ParallelFlash(self.conf, self.recorder, self.global_helper)

        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)

        ###### the managers ######
        self.mapping_manager = MappingManager(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = self.flash,
            oobobj=self.oob,
            recorderobj = recorderobj
            )

        self.garbage_collector = GarbageCollector(
            confobj = self.conf,
            flashobj = self.flash,
            oobobj=self.oob,
            block_pool = self.block_pool,
            mapping_manager = self.mapping_manager,
            recorderobj = recorderobj
            )

        # We should initialize Globaltranslationdirectory in Dftl
        self.mapping_manager.initialize_mappings()

        self.n_sec_per_page = self.conf.page_size \
                / self.conf['sector_size']

    def lba_discard(self, lpn, pid = None):
        """
        block_pool:
            no need to update
        CMT:
            if lpn->ppn exist, you need to update it to lpn->UNINITIATED
            if not exist, you need to add lpn->UNINITIATED
            the mapping lpn->UNINITIATED will be written back to GMT later
        GMT:
            no need to update
            REMEMBER: all updates to GMT can and only can be maded through CMT
        OOB:
            invalidate the ppn
            remove the lpn
        GTD:
            no updates needed
            updates should be done by GC
        """
        self.recorder.put('logical_discard', lpn, 'user')

        # self.recorder.write_file('lba.trace.txt',
            # timestamp = self.oob.timestamp(),
            # operation = 'discard',
            # lpn =  lpn
        # )

        ppn = self.mapping_manager.lpn_to_ppn(lpn)
        if ppn == UNINITIATED:
            return

        # flash page ppn has valid data
        self.mapping_manager.cached_mapping_table.overwrite_entry(lpn = lpn,
            ppn = UNINITIATED, dirty = True)

        # OOB
        self.oob.wipe_ppn(ppn)

        # garbage collection checking and possibly doing
        # self.garbage_collector.try_gc()

    def sec_read(self, sector, count):
        """
        There are two parts here.
        1. Translation all LPN to PPN
        2. Read the PPNs in parallel

        Note that the tranlation may incur GC
        It returns an array of data.
        """
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)
        self.global_helper.timeline.add_logical_op(sector = sector, count = count,
                op = 'LOGICAL_READ')

        ppns_to_read = self.mapping_manager.ppns_for_reading(
            range(lpn_start, lpn_start + lpn_count))

        data = self.flash.read_pages(ppns = ppns_to_read, tag = DATA_USER)
        data = self.page_to_sec_items(data)

        self.check_read(sector, count, data)

        return data

    def check_read(self, sector, sector_count, data):
        for sec, sec_data in zip(
                range(sector, sector + sector_count), data):
            if sec_data == None:
                continue
            if not sec_data.startswith(str(sec)):
                msg = "request: sec {} count {}\n".format(sector, sector_count)
                msg += "INFTL: Data is not correct. Got: {read}, "\
                        "sector={sec}".format(
                        read = sec_data,
                        sec = sec)
                # print msg
                raise RuntimeError(msg)

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

    def sec_write(self, sector, count, data = None):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        self.global_helper.timeline.add_logical_op(sector = sector, count = count,
                op = 'LOGICAL_WRITE')

        ppns_to_write = self.mapping_manager.ppns_for_writing(
            range(lpn_start, lpn_start + lpn_count))

        ppn_data = self.sec_to_page_items(data)
        self.flash.write_pages(ppns = ppns_to_write, ppn_data = ppn_data,
                tag = DATA_USER)

        self.garbage_collector.try_gc()

    def sec_discard(self, sector, count):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        self.global_helper.timeline.add_logical_op(sector = sector, count = count,
                op = 'LOGICAL_DISCARD')

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.lba_discard(lpn)

    def pre_workload(self):
        self.global_helper.timeline.turn_on()

    def post_processing(self):
        """
        This function is called after the simulation.
        """
        self.global_helper.timeline.save()

    def get_type(self):
        return "dftlext"


def assert_oob_lpn_eq_req_lpn(mapping_manager, oob, ppn, req_lpn):
    if ppn == 'UNINIT':
        return

    if ppn == None:
        return

    oob_lpn = oob.translate_ppn_to_lpn(ppn)
    if oob_lpn != req_lpn:
        msg = "oob_lpn {oob_lpn} != req_lpn {req_lpn}. PPN={ppn}\n"\
            .format(oob_lpn = oob_lpn, req_lpn = req_lpn, ppn = ppn)
        msg += "Mapping: lpn {} -> ppn {}.\n".format(
            5434, mapping_manager.lpn_to_ppn(5434))
        msg += "Mapping: lpn {} -> ppn {}.\n".format(
            6127, mapping_manager.lpn_to_ppn(6127))

        raise RuntimeError(msg)


def assert_flash_data_startswith_oob_lpn(conf, flash, oob, ppn):
    if ppn == None:
        return

    if ppn == 'UNINIT':
        return

    flashdata = flash.flash_backend.data
    oob_lpn = oob.translate_ppn_to_lpn(ppn)
    sec, sec_count = conf.page_ext_to_sec_ext(oob_lpn, 1)
    for sec_num, data in zip(range(sec, sec+sec_count), flashdata[ppn]):
        if not data.startswith(str(sec_num)):
            msg = "Flash data does not match its stored sec num"\
                "flash data: {}, ppn: {}. oob_lpn: {} sec:{}".format(
                flashdata[ppn], ppn, oob_lpn,
                list(range(sec, sec+sec_count)) )
            print msg
            raise RuntimeError(msg)

def check_data(mapping_manager, conf, flash, oob, ppn, req_lpn = None):
    assert_oob_lpn_eq_req_lpn(mapping_manager, oob, ppn, req_lpn)
    assert_flash_data_startswith_oob_lpn(conf, flash, oob, ppn)


class ParallelFlash(object):
    def __init__(self, confobj, recorderobj, globalhelper = None):
        self.conf = confobj
        self.recorder = recorderobj
        self.global_helper = globalhelper
        self.flash_backend = flash.SimpleFlash(recorderobj, confobj)

    def get_max_channel_page_count(self, ppns):
        """
        Find the max count of the channels
        """
        pbns = []
        for ppn in ppns:
            if ppn == 'UNINIT':
                # skip it so unitialized ppn does not involve flash op
                continue
            block, _ = self.conf.page_to_block_off(ppn)
            pbns.append(block)

        return self.get_max_channel_block_count(pbns)

    def get_max_channel_block_count(self, pbns):
        channel_counter = Counter()
        for pbn in pbns:
            channel, _ = block_to_channel_block(self.conf, pbn)
            channel_counter[channel] += 1

        return self.find_max_count(channel_counter)

    def find_max_count(self, channel_counter):
        if len(channel_counter) == 0:
            return 0
        else:
            max_channel, max_count = channel_counter.most_common(1)[0]
            return max_count

    def read_pages(self, ppns, tag):
        """
        Read ppns in batch and calculate time
        lpns are the corresponding lpns of ppns, we pass them in for checking
        """
        max_count = self.get_max_channel_page_count(ppns)
        self.global_helper.timeline.incr_time_stamp('flash.read',
                max_count)

        data = []
        for ppn in ppns:
            data.append( self.flash_backend.page_read(ppn, tag) )
        return data

    def write_pages(self, ppns, ppn_data, tag):
        """
        This function will store ppn_data to flash and calculate the time
        it takes to do it with real flash.

        The access time is determined by the channel with the longest request
        queue.
        """
        max_count = self.get_max_channel_page_count(ppns)
        self.global_helper.timeline.incr_time_stamp('flash.write',
                max_count)

        # save the data to flash
        if ppn_data == None:
            for ppn in ppns:
                self.flash_backend.page_write(ppn, tag)
        else:
            for ppn, item in zip(ppns, ppn_data):
                self.flash_backend.page_write(ppn, tag, data = item)

    def erase_blocks(self, pbns, tag):
        max_count = self.get_max_channel_block_count(pbns)
        self.global_helper.timeline.incr_time_stamp('flash.erasure',
                max_count)

        for block in pbns:
            self.flash_backend.block_erase(block, cat = tag)


