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
    - it manages the appending point for different purpose. No, it does not have
    enough info to do that.

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
          are (so you can read/write), also, you need to update GTD when evicting
          pages to flash.
        - bitmap???

- Global mapping table (GMT): this class holds the mappings of all the data pages. In
  real implementation, this should be stored in flash. This data structure will be used
  intensively. It should have a good interface.
    - API, get_entries_of_Mvpn(virtual mapping page number). This is one of the
      cases that it may be used: when reading a data page, its translation entry is not
      in CMT. The translator will consult the GTD to find the physical
      translation page number of virtual translation page number V. Then we need to get the
      content of V by reading its corresponding physical page. We may provide an interface
      to the translator like load_translation_physical_page(PPN), which internally, we
      read from GMT.

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


class GlobalMappingTable(object):
    """
    This mapping table is for data pages, not for translation pages.
    """
    def __init__(self, confobj, flashobj):
        """
        flashobj is the flash device that we may operate on.
        """
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not conf.Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj

        # bidirection mapping
        self.l2p = bidict.bidict()

    def has_logical_page(self, logical_page):
        return logical_page in self.l2p

    def has_physical_page(self, physical_page):
        return physical_page in self.l2p.inv

    def logical_to_physical_page(self, logical_page):
        return self.l2p[logical_page]

    def physical_to_logical_page(self, physical_page):
        return self.l2p[:physical_page]

        if flash_page != None:
            del self.log_page_l2p[:flash_page]

        self.rec.put_and_count("remove_log_page_mapping", lba_page, flash_page)


class Dftl(ftlbuilder.FtlBuilder):
    """
    The implementation literally follows DFtl paper.
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(HybridMapFtl, self).__init__(confobj, recorderobj, flashobj)

        # bitmap has been created parent class
        self.bitmap.initialize()

        # initialize free list
        self.block_pool = BlockPool(self.conf['flash_num_blocks'])

    # FTL APIs
    def lba_read(self, pagenum):
        pass

    def lba_write(self, pagenum):
        pass

    def lba_discard(self, pagenum):
        pass

    #





