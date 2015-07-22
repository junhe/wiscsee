import bitarray
from collections import deque
import random
import os

import bidict

import config
import ftlbuilder
import recorder

"""
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
"""

UNINITIATED, MISS = ('UNINIT', 'MISS')
DATA_BLOCK, TRANS_BLOCK = ('data_block', 'trans_block')

debugrec = recorder.Recorder(recorder.STDOUT_TARGET, verbose_level = 3)
def db(*args):
    debugrec.debug(*args)


class BlockPool(object):
    def __init__(self, confobj):
        self.conf = confobj

        self.freeblocks = deque(range(self.conf['flash_num_blocks']))

        # initialize usedblocks
        self.trans_usedblocks = []
        self.data_usedblocks  = []

    def pop_a_free_block(self):
        if self.freeblocks:
            blocknum = self.freeblocks.popleft()
        else:
            # nobody has free block
            raise RuntimeError('No free blocks in device!!!!')

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
        self.trans_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def total_used_blocks(self):
        return len(self.trans_usedblocks) + len(self.data_usedblocks)

    def used_blocks(self):
        return self.trans_usedblocks + self.data_usedblocks

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

    def current_blocks(self):
        cur_data_block, _ = self.conf.page_to_block_off(
            self.data_log_end_ppn)
        cur_trans_block, _ = self.conf.page_to_block_off(
            self.trans_log_end_ppn)
        return (cur_data_block, cur_trans_block)

    def __repr__(self):
        ret = ' '.join(['freeblocks', repr(self.freeblocks)]) + '\n' + \
            ' '.join(['trans_usedblocks', repr(self.trans_usedblocks)]) + \
            '\n' + \
            ' '.join(['data_usedblocks', repr(self.data_usedblocks)])
        return ret

    def visual(self):
        block_states = [ 'O' if block in self.freeblocks else 'X'
                for block in range(self.conf['flash_num_blocks'])]
        return ''.join(block_states)


class CacheEntryData(object):
    """
    This is a helper class that store entry data for a LPN
    """
    ppn = None
    dirty = None # True or False

    def __init__(self, ppn, dirty):
        self.ppn = ppn
        self.dirty = dirty

    def __repr__(self):
        return "ppn {}, dirty {}".format(self.ppn, self.dirty)

class CachedMappingTable(object):
    def __init__(self, confobj):
        self.conf = confobj

        # let's begin by using simple dict, more advanced structure needed
        # later it holds lpn->ppn
        self.entries = {}

        self.entry_bytes = 64 # lpn + ppn
        max_bytes = self.conf['dftl']['max_cmt_bytes']
        self.max_n_entries = (max_bytes + self.entry_bytes - 1) / \
            self.entry_bytes

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
        self.entries[lpn] = CacheEntryData(ppn = ppn, dirty = dirty)

    def update_entry(self, lpn, ppn, dirty):
        "You may end up remove the old one"
        self.entries[lpn] = CacheEntryData(ppn = ppn, dirty = dirty)

    def overwrite_entry(self, lpn, ppn, dirty):
        "lpn must exist"
        self.entries[lpn].ppn = ppn
        self.entries[lpn].dirty = dirty

    def remove_entry_by_lpn(self, lpn):
        del self.entries[lpn]

    def victim_entry(self):
        lpn = random.sample(self.entries, 1)

        # lpn, Cacheentrydata
        return lpn, self.entries[lpn]

    def is_full(self):
        n = len(self.entries)
        assert n <= self.max_n_entries
        return n == self.max_n_entries

    def new_data_write_event(self, lpn, new_ppn):
        """
        This method should be used when there is a new write. The new mapping
        is lpn->new_ppn.

        if lpn's mapping entry is in cache, update it and mark it as
        dirty. If it is not in cache, add such entry and mark as dirty.

        when updating, do we need to keep the old mapping?
        This relates to how we write back the entries in cache back go GMT.
        Check GMT.write_back_cache for details.
        Basically, we need only to keep track of the latest lpn->ppn in cache.
        If you have overwritten one lpn for many times, those lpn->ppn you
        discarded are not really lost, they are stored in OOB as ppn->lpn by
        ftl.lba_write().
        """
        self.overwrite_entry(lpn = lpn, ppn = new_ppn, dirty = True)

    def __repr__(self):
        return repr(self.entries)


class GlobalMappingTable(object):
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

        self.n_entries_per_page = self.conf.dft_n_mapping_entries_per_page()

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
        entry_bytes = self.conf['dftl']['global_mapping_entry_bytes']
        flash_page_size = self.conf['flash_page_size']
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

        self.flash_npage_per_block = self.conf['flash_npage_per_block']
        self.flash_num_blocks = self.conf['flash_num_blocks']
        self.flash_page_size = self.conf['flash_page_size']
        self.total_pages = self.conf.total_num_pages()

        self.n_entries_per_page = self.conf.dft_n_mapping_entries_per_page()

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

    def m_ppn_of_lpn(self, lpn):
        m_vpn = self.m_vpn_of_lpn(lpn)
        m_ppn = self.m_vpn_to_m_ppn(m_vpn)
        return m_ppn

    def __repr__(self):
        return repr(self.mapping)

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

        self.flash_num_blocks = confobj['flash_num_blocks']
        self.flash_npage_per_block = confobj['flash_npage_per_block']
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        # Key data structures
        self.states = ftlbuilder.FlashBitmap2(confobj)
        # ppn->lpn mapping stored in OOB
        self.ppn_to_lpn = {}

    def translate_ppn_to_lpn(self, ppn):
        return self.ppn_to_lpn[ppn]

    def apply_event(self, event):
        pass

    def wipe_ppn(self, ppn):
        self.states.invalidate_page(ppn)
        del self.ppn_to_lpn[ppn]

    def erase_block(self, flash_block):
        self.states.erase_block(flash_block)

        start, end = self.conf.block_to_page_range(flash_block)
        for ppn in range(start, end):
            try:
                del self.ppn_to_lpn[ppn]
            except KeyError:
                pass

    def new_write(self, lpn, old_ppn, new_ppn):
        """
        mark the new_ppn as valid
        update the LPN in new page's OOB to lpn
        invalidate the old_ppn, go cleaner can GC it
        """
        self.states.validate_page(new_ppn)
        self.ppn_to_lpn[new_ppn] = lpn

        if old_ppn != UNINITIATED:
            # the lpn has mapping before this write
            self.wipe_ppn(old_ppn)

class MappingManager(object):
    """
    This class is the supervisor of all the mappings. When initializing, it
    register CMT and GMT with it and provides higher level operations on top of
    them.
    This class should act as a coordinator of all the mapping data structures.
    """
    def __init__(self, cached_mapping_table, global_mapping_table,
        global_translation_directory, block_pool, confobj, flashobj, oobobj):
        self.conf = confobj

        self.cached_mapping_table = cached_mapping_table
        self.global_mapping_table = global_mapping_table
        self.directory = global_translation_directory

        self.block_pool = block_pool

        self.flash = flashobj
        self.oob = oobobj

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
            if self.cached_mapping_table.is_full():
                self.evict_cache_entry()

            # find the physical translation page holding lpn's mapping in GTD
            ppn = self.load_mapping_entry_to_cache(lpn)

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
        self.flash.page_read(m_ppn, 'amplified')

        # Now we have all the entries of m_ppn in memory, we need to put
        # the mapping of lpn->ppn to CMT
        ppn = self.global_mapping_table.lpn_to_ppn(lpn)
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
        total_pages = self.global_mapping_table.total_translation_pages()

        # use some free blocks to be translation blocks
        tmp_blk_mapping = {}
        for m_vpn in range(total_pages):
            m_ppn = self.block_pool.next_translation_page_to_program()
            # Note that we don't actually read or write flash
            self.directory.add_mapping(m_vpn=m_vpn, m_ppn=m_ppn)
            # update oob of the translation page
            self.oob.new_write(lpn = m_vpn, old_ppn = UNINITIATED,
                new_ppn = m_ppn)


    def write_back_cache(self):
        """
        It write the dirty entries in CMT back to GMT. This is how it works:
        Note: 'dirty' means the entry in cache is differnt to the one in GMT
        for entry in entrylist:
            if entry is dirty:
                GMT
                load entry's LPN's translation page from GMT to memory
                update LPN's entry in translation page
                write the updated page to a free page
                GTD
                update GTD to redirect LPN's VPN to new PPN
                OOB
                invalidate the old page in OOB
                validate new PPN in its OOB
                write vpn to OOB
                CMT
                mark entry as clean
        """
        pass

    def update_entry(self, lpn, new_ppn):
        """
        update entry of lpn to be lpn->new_ppn everywhere if necessary

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
        old_m_ppn = self.directory.m_vpn_to_m_ppn(lpn)

        # update GMT on flash
        self.flash.page_read(old_m_ppn, 'amplified')
        pass # modify in memory
        new_m_ppn = self.block_pool.next_translation_page_to_program()
        self.flash.page_write(new_m_ppn, 'amplified')
        # update our fake 'on-flash' GMT
        self.global_mapping_table.update(lpn = lpn, ppn = ppn)

        # OOB
        self.oob.new_write(lpn = m_vpn, old_ppn = old_m_ppn,
            new_ppn = new_m_ppn)

        # update GTD so we can find it
        self.directory.update_mapping(m_vpn = m_vpn, m_ppn = new_m_ppn)

    def evict_cache_entry(self):
        """
        Select one entry in cache
        If the entry is dirty, write it back to GMT.
        If it is not dirty, simply remove it.
        """
        vic_lpn, vic_entrydata = self.cached_mapping_table.victim_entry()

        if vic_entrydata.dirty == True:
            # update every data structure related
            self.update_entry(vic_lpn, vic_entrydata.ppn)

        # remove the entry
        self.cached_mapping_table.remove_entry_by_lpn(vic_lpn)

class GcDecider(object):
    """
    It is used to decide wheter we should do garbage collection.

    When need_cleaning() is called the first time, use high water mark
    to decide if we need GC.
    Later, use low water mark and progress to decide. If we haven't make
    progress in 10 times, stop GC
    """
    def __init__(self, confobj, block_pool):
        self.conf = confobj
        self.block_pool = block_pool
        self.call_index = -1

        self.high_watermark = self.conf['dftl']['GC_threshold_ratio'] * \
            self.conf['flash_num_blocks']
        self.low_watermark = self.conf['dftl']['GC_low_threshold_ratio'] * \
            self.conf['flash_num_blocks']

        self.last_used_blocks = None
        self.freeze_count = 0

    def need_cleaning(self):
        "The logic is a little complicated"
        self.call_index += 1

        n_used_blocks = self.block_pool.total_used_blocks()

        if self.call_index == 0:
            # clean when above high_watermark
            ret = n_used_blocks > self.high_watermark
        else:
            if self.freezed_too_long(n_used_blocks):
                ret = False
            else:
                # common case
                ret = n_used_blocks > self.low_watermark

        return ret

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

            if self.freeze_count > self.conf['flash_npage_per_block']:
                ret = True
            else:
                ret = False

        if ret == True:
            print 'free too long', self.freeze_count
        return ret

class Dftl(ftlbuilder.FtlBuilder):
    """
    The implementation literally follows DFtl paper.
    This class is a coordinator of other coordinators and data structures
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(Dftl, self).__init__(confobj, recorderobj, flashobj)

        # bitmap has been created parent class
        # Change: we now don't put the bitmap here
        # self.bitmap.initialize()
        del self.bitmap

        # initialize free list
        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)

        self.global_mapping_table = GlobalMappingTable(confobj, flashobj)
        self.cached_mapping_table = CachedMappingTable(confobj)
        self.global_translation_directory = GlobalTranslationDirectory(confobj)


        # register the mapping data structures with the manger
        # later you may also need to register them with the cleaner
        self.mapping_manager = MappingManager(
            cached_mapping_table = self.cached_mapping_table,
            global_mapping_table = self.global_mapping_table,
            global_translation_directory = self.global_translation_directory,
            block_pool = self.block_pool, confobj = self.conf,
            flashobj = flashobj, oobobj=self.oob)

        # We should initialize Globaltranslationdirectory in Dftl
        self.mapping_manager.initialize_mappings()

    def __del__(self):
        print self.cached_mapping_table

    # FTL APIs
    def lba_read(self, lpn):
        """
        ppn = translate(pagenum))
        flash.read(ppn)
        """
        self.recorder.put('lba_read', lpn, 'user')

        ppn = self.mapping_manager.lpn_to_ppn(lpn)
        self.flash.page_read(ppn, 'user')

    def lba_write(self, lpn):
        """
        This is the interface for higher level to call, do NOT use it for
        internal use. If you need, create new one and refactor the code.

        block_pool
            no need to update
        CMT
            if lpn's mapping entry is in cache, update it and mark it as
            dirty. If it is not in cache, add such entry and mark as dirty
        GMT
            no need to update, it will be updated when we write back CMT
        OOB
            mark the new_ppn as valid
            update the LPN to lpn
            invalidate the old_ppn, go cleaner can GC it
            TODO: should the DFtl paper have considered the operation in OOB
        GTD
            No need to update, because GMT does not change
        Garbage collector
            We need to check if we need to do garbage collection
        Appending point
            It is automatically updated by next_data_page_to_program
        Flash
        """
        self.recorder.put('lba_write', lpn, 'user')

        old_ppn = self.mapping_manager.lpn_to_ppn(lpn)

        # appending point
        new_ppn = self.block_pool.next_data_page_to_program()

        # CMT
        self.cached_mapping_table.new_data_write_event(lpn = lpn,
            new_ppn = new_ppn)

        # OOB
        self.oob.new_write(lpn = lpn, old_ppn = old_ppn,
            new_ppn = new_ppn)

        # Flash
        self.flash.page_write(new_ppn, 'user')

        # garbage collection
        self.gc()

        db( self.block_pool.visual() )

    def lba_discard(self, lpn):
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
        self.recorder.put('lba_discard', lpn, 'user')

        ppn = self.mapping_manager.lpn_to_ppn(lpn)
        if ppn == UNINITIATED:
            return

        # flash page ppn has valid data
        self.cached_mapping_table.overwrite_entry(lpn = lpn, ppn = UNINITIATED,
            dirty = True)

        # OOB
        self.oob.wipe_ppn(ppn)

    def erase_block(self, blocknum):
        """
        set pages' oob states to ERASED
        electrionically erase the pages
        """
        # set page states to ERASED and in-OOB lpn to nothing
        self.oob.erase_block(blocknum)

        self.flash.block_erase(blocknum, 'amplified')

    def gc(self):
        debugrec.debug('start gc.............................................')
        decider = GcDecider(self.conf, self.block_pool)
        while decider.need_cleaning():
            victim_type, victim_block, valid_ratio = self.next_victim_block()
            print 'gc loop----->', victim_type, victim_block, valid_ratio
            db(self.block_pool)
            if valid_ratio == 1:
                # all blocks are full of valid pages
                break
            if victim_type == DATA_BLOCK:
                self.clean_data_block(victim_block)
            elif victim_type == TRANS_BLOCK:
                self.clean_trans_block(victim_block)
        debugrec.debug('end gc..............................................')

    def clean_data_block(self, flash_block):
        debugrec.debug('clean_data_block', flash_block)
        self.move_valid_pages(flash_block, self.move_data_page_to_new_location)
        # mark block as free
        self.block_pool.move_used_data_block_to_free(flash_block)

    def clean_trans_block(self, flash_block):
        debugrec.debug('clean_trans_block', flash_block)
        self.move_valid_pages(flash_block,
            self.move_trans_page_to_new_location)
        # mark block as free
        self.block_pool.move_used_trans_block_to_free(flash_block)

    def move_valid_pages(self, flash_block, mover_func):
        start, end = self.conf.block_to_page_range(flash_block)

        for ppn in range(start, end):
            debugrec.debug('check ppn:', ppn)
            if self.oob.states.is_page_valid(ppn):
                debugrec.debug('move ppn:', ppn)

                assert self.oob.states.is_page_valid(ppn)
                print self.oob.states.page_state_human(ppn)
                mover_func(ppn)
                assert self.oob.states.is_page_invalid(ppn)
                print self.oob.states.page_state_human(ppn)

    def move_data_page_to_new_location(self, ppn):
        """
        Page ppn must be valid.
        This function is for garbage collection. The difference between this
        one and the lba_write is that the input of this function is ppn, while
        lba_write's is lpn. Because of this, we don't need to consult mapping
        table to find the mapping. The lpn->ppn mapping is stored in OOB.
        Another difference is that, if the mapping exists in cache, update the
        entry in cache. If not, update the entry in GMT (and thus GTD).
        """
        # for my damaged brain
        old_ppn = ppn

        # read the the data page
        self.flash.page_read(old_ppn, 'amplified')

        # find the mapping
        lpn = self.oob.translate_ppn_to_lpn(old_ppn)

        # write to new page
        new_ppn = self.block_pool.next_data_page_to_program()
        self.flash.page_write(new_ppn, 'amplified')

        # update new page and old page's OOB
        self.oob.new_write(lpn, old_ppn, new_ppn)

        cached_ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        if cached_ppn == MISS:
            # This will not add mapping to cache
            self.mapping_manager.update_entry(lpn = lpn, new_ppn = new_ppn)
        else:
            # lpn is in cache, update it
            # This is a design from the original Dftl paper
            self.cached_mapping_table.overwrite_entry(lpn = lpn, ppn = new_ppn,
                dirty = True)

    def move_trans_page_to_new_location(self, m_ppn):
        """
        1. read the trans page
        2. write to new location
        3. update OOB
        4. update GTD
        """
        old_m_ppn = m_ppn

        m_vpn = self.oob.translate_ppn_to_lpn(old_m_ppn)

        self.flash.page_read(old_m_ppn, 'amplified')

        # write to new page
        new_m_ppn = self.block_pool.next_translation_page_to_program()
        self.flash.page_write(new_m_ppn, 'amplified')

        # update new page and old page's OOB
        self.oob.new_write(m_vpn, old_m_ppn, new_m_ppn)

        # update GTD
        self.global_translation_directory.update_mapping(m_vpn = m_vpn,
            m_ppn = new_m_ppn)

    def next_victim_block(self):
        "TODO: refactor the code"
        min_valid_ratio = 2
        ret_block = None
        block_type = None

        current_blocks = self.block_pool.current_blocks()

        for blocknum in self.block_pool.data_usedblocks:
            if blocknum in current_blocks:
                continue

            valid_ratio = self.oob.states.block_valid_ratio(blocknum)
            if valid_ratio < min_valid_ratio:
                block_type = DATA_BLOCK
                ret_block = blocknum
                min_valid_ratio = valid_ratio

        for blocknum in self.block_pool.trans_usedblocks:
            if blocknum in current_blocks:
                continue

            valid_ratio = self.oob.states.block_valid_ratio(blocknum)
            if valid_ratio < min_valid_ratio:
                block_type = TRANS_BLOCK
                ret_block = blocknum
                min_valid_ratio = valid_ratio

        if ret_block == None:
            self.recorder.debug("no block is used yet.")

        # print maxblock, maxratio
        return block_type, ret_block, min_valid_ratio

def main():
    pass

if __name__ == '__main__':
    main()

