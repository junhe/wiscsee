import bitarray
from collections import deque
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
    - NOTE2: When cleaning a victim block, we need to know the corresponding logical
    page number of a vadlid physical page in the block. However, the Global Mapping
    Table does not provide physical to logical mapping. We can maintain such an
    mapping table by our own and assume that information is stored in OOB.

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


class BlockPool(object):
    def __init__(self, num_blocks):
        self.freeblocks = deque(range(num_blocks))

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

    def __repr__(self):
        ret = ' '.join('freeblocks', repr(self.freeblocks)) + '\n' + \
            ' '.join('trans_usedblocks', repr(self.trans_usedblocks)) + '\n' \
            ' '.join('data_usedblocks', repr(self.data_usedblocks)) + '\n'

class CachedMappingTable(object):
    """
    """
    def __init__(self, confobj):
        # let's begin by using simple dict, more advanced structure needed later
        # it holds lpn->ppn
        self.entries = {}

    def lpn_to_ppn(self, lpn):
        "Try to find ppn of the given lpn in cache"
        return self.entries.get(lpn, None)

    def add_entry(self, lpn, ppn):
        if self.entries.has_key(lpn):
            raise RuntimeError("{}:{} already exists in CMT entries.".format(
                lpn, self.entries[lpn]))
        self.entries[lpn] = ppn

    def remove_entry_by_lpn(self, lpn):
        del self.entries[lpn]

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
        return (self.total_entries() * \
            self.conf['dftl']['global_mapping_entry_bytes']) / \
            self.conf['flash_page_size']


class GlobalTranslationDirectory(object):
    def __init__(self, confobj):
        self.conf = confobj

        self.flash_npage_per_block = self.conf['flash_npage_per_block']
        self.flash_num_blocks = self.conf['flash_num_blocks']
        self.flash_page_size = self.conf['flash_page_size']
        self.total_pages = self.conf.total_num_pages()

        self.entry_bytes = self.conf['dftl']['global_mapping_entry_bytes']
        self.n_entries_per_page = self.flash_page_size / self.entry_bytes

        # M_VPN -> M_PPN
        # Virtual translation page number --> Physical translation page number
        # Dftl should initialize
        self.mapping = {}

    def m_vpn_to_m_ppn(self, m_vpn):
        "m_vpn virtual translation page number"
        return self.mapping.get(m_vpn, None)

    def add_mapping(self, m_vpn, m_ppn):
        if self.mapping.has_key(m_vpn):
            raise RuntimeError("self.mapping already has m_vpn:{}".format(m_vpn))
        self.mapping[m_vpn] = m_ppn


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
        self.flash_num_blocks = confobj['flash_num_blocks']
        self.flash_npage_per_block = confobj['flash_npage_per_block']
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        self.states = ftlbuilder.FlashBitmap2(confobj)
        self.lpn_of_phy_page = {} #

    def apply_event(self, event):
        pass

class MappingManager(object):
    """
    This class is the supervisor of all the mappings. When initializing, it
    register CMT and GMT with it and provides higher level operations on top of
    them.
    """
    def __init__(self, cached_mapping_table, global_mapping_table,
        global_translation_directory, block_pool, confobj):
        self.conf = confobj

        self.cached_mapping_table = cached_mapping_table
        self.global_mapping_table = global_mapping_table
        self.directory = global_translation_directory

        self.block_pool = block_pool

    def lpn_to_ppn(self, lpn):
        """
        This method does not fail. It will try everything to find the ppn of
        the given lpn.
        """
        # try cached mapping table first.
        ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        if ppn == None:
            # cache miss
            # TODO:evict one entry from CMT

            # find the physical translation page holding lpn's mapping
            # Load mapping for lpn from global table
            pass

        # now the cache should have the ppn, try it again
        ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        assert ppn != None
        return ppn

    def initialize_mappings(self):
        """
        This function initialize all global mapping table and global mapping
        directory. We assume the GTD is very small and stored in flash before
        mounting. We also assume that the global mapping table has been prepared
        by the vendor, so there is no other overhead except for reading the GTD
        from flash. Since the overhead is very small, we ignore it.
        """
        total_pages = self.global_mapping_table.total_translation_pages()

        # use some free blocks to be translation blocks
        tmp_blk_mapping = {}
        for m_vpn in range(total_pages):
            vblock, off = self.conf.page_to_block_off(m_vpn)
            if not tmp_blk_mapping.has_key(vblock):
                phy_block = self.block_pool.pop_a_free_block_to_trans()
                tmp_blk_mapping[vblock] = phy_block
            phy_block = tmp_blk_mapping[vblock]
            m_ppn = self.conf.block_off_to_page(phy_block, off)

            self.directory.add_mapping(m_vpn=m_vpn, m_ppn=m_ppn)

class Dftl(ftlbuilder.FtlBuilder):
    """
    The implementation literally follows DFtl paper.
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(Dftl, self).__init__(confobj, recorderobj, flashobj)

        # bitmap has been created parent class
        # Change: we now don't put the bitmap here
        # self.bitmap.initialize()
        del self.bitmap

        # initialize free list
        self.block_pool = BlockPool(self.conf['flash_num_blocks'])

        self.global_mapping_table = GlobalMappingTable(confobj, flashobj)
        self.cached_mapping_table = CachedMappingTable(confobj)
        self.global_translation_directory = GlobalTranslationDirectory(confobj)

        # register the mapping data structures with the manger
        # later you may also need to register them with the cleaner
        self.mapping_manager = MappingManager(
            cached_mapping_table = self.cached_mapping_table,
            global_mapping_table = self.global_mapping_table,
            global_translation_directory = self.global_translation_directory,
            block_pool = self.block_pool, confobj = self.conf)

        # TODO: we should initialize Globaltranslationdirectory in Dftl
        # By default, let's use the first n pages of the translation
        self.mapping_manager.initialize_mappings()

        self.oob = OutOfBandAreas(confobj)

    # FTL APIs
    def lba_read(self, lpn):
        """
        ppn = translate(pagenum))
        flash.read(ppn)
        """
        ppn = self.mapping_manager.lpn_to_ppn(lpn)
        self.flash.page_read(ppn, 'user')

    def lba_write(self, lpn):
        """
        """
        pass

    def lba_discard(self, lpn):
        """
        oob.apply_event(event)
        This should invalidate the page and remove the LPN in OOB
        """
        pass

    def lpn_to_ppn(self, lpn):
        pass


def main():
    pass

if __name__ == '__main__':
    main()

