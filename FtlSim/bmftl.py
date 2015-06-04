from collections import deque

import bitarray

from common import *
import config
import flash
import recorder

class Ftl:
    """
    When write a page P:
    1. check if P's block is in device
        if no, take a new block from free block list and write all the pages
            until P, sequentially
        if yes, read all the valid pages from the block, modify it in memory,
            then write the pages sequentially back, until the last valid page

    When garbage collecting,
    1. find a block with NO valid page, erase the block
    """

    def __init__(self, page_size, npages_per_block, num_blocks):
        self.page_size = page_size
        self.npages_per_block = npages_per_block
        self.flash_num_blocks = num_blocks

        # initialize bitmap 1: valid, 0: invalid
        # valid means a page has data, invalid means it has garbage
        npages = num_blocks * npages_per_block
        self.validbitmap = bitarray.bitarray(npages)
        self.validbitmap.setall(False)

        self.blk_l2p = {} # logical (LBA) block (multiple pages)
                      # to physical (flash) block
        self.blk_p2l = {}

        # this should be maitained a queue
        # later we may maitain it as a set, where we can pick a optimal
        # block as the next block for wear leveling
        # we can check self.freeblocks to see if we need do garbage collection
        self.freeblocks = deque(range(self.flash_num_blocks))
        self.usedblocks = []

        # trigger garbage collectionif the number of free blocks is below
        # the number below
        self.low_num_blocks = 0.5 * self.flash_num_blocks

    # bitmap operations
    def validate_flash_page(self, pagenum):
        "use this function to wrap the operation, "\
        "in case I change bitmap module later"
        self.validbitmap[pagenum] = True

    def invalidate_flash_page(self, pagenum):
        "mark the bitmap and remove flashpage -> lba mapping"
        self.validbitmap[pagenum] = False

    def validate_flash_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = True

    def invalidate_flash_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = False

    # basic operations
    def read_page(self, pagenum, cat):
        flash.page_read(pagenum, cat)

    def read_block(self, blocknum, cat):
        start, end = block_to_page_range(blocknum)
        self.read_page_range(start, end, cat)

    def read_page_range(self, start, end, cat):
        for pagenum in range(start, end):
            flash.page_read(pagenum, cat)

    def program_block(self, blocknum, cat):
        start, end = block_to_page_range(blocknum)
        for pagenum in range(start, end):
            flash.page_write(pagenum, cat)

    def program_page_range(self, start, end, cat):
        for pagenum in range(start, end):
            flash.page_write(pagenum, cat)

    def erase_block(self, blocknum, cat):
        flash.block_erase(blocknum, cat)

    def modify_page_in_ram(self, pagenum):
        "this is a dummy function"
        pass

    def pop_a_free_block(self):
        # take a block from free block queue
        # assert len(self.freeblocks) > 0, 'No free blocks in device!!!'
        if len(self.freeblocks) == 0:
            recorder.error('No free blocks in device!!!!')
            exit(1)

        blocknum = self.freeblocks.popleft()
        # now the block is busy, we put it to the used list.
        # a block is either in the used list or the free list
        self.usedblocks.append(blocknum)
        return blocknum

    def append_a_free_block(self, blocknum):
        "Note that it removes block num from the used list"
        self.usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def show_map(self):
        recorder.debug(self.blk_l2p)
        recorder.debug(self.blk_p2l)

    def lba_page_to_flash_page(self, lbapagenum):
        lba_block, lba_off = page_to_block_off(lbapagenum)
        if self.blk_l2p.has_key(lba_block):
            flash_block = self.blk_l2p[lba_block]
            return block_off_to_page(flash_block, lba_off)
        else:
            return None

    def flash_page_to_lba_page(self, flash_page):
        flash_block, flash_off = page_to_block_off(flash_page)
        if self.blk_p2l.has_key(flash_block):
            lba_block = self.blk_p2l[flash_block]
            return block_off_to_page(lba_block, flash_off)
        else:
            return None

    def remove_mapping_by_lba_block(self, lba_block):
        flash_block = self.blk_l2p[lba_block]
        del self.blk_l2p[lba_block]
        del self.blk_p2l[flash_block]

    def remove_mapping_by_flash_block(self, flash_block):
        lba_block = self.blk_p2l[flash_block]
        del self.blk_l2p[lba_block]
        del self.blk_p2l[flash_block]

    def invalidate_lba_page(self, lbapagenum):
        "invalidate bitmap and remove the mapping"

        flash_page = self.lba_page_to_flash_page(lbapagenum)

        if flash_page != None:
            # the lba has a corresponding flash page on device
            self.invalidate_flash_page(flash_page)
        else:
            recorder.warning('trying to invalidate a page not in block map')

    def read_valid_pages(self, blocknum, cat):
        start, end = block_to_page_range(blocknum)
        for page in range(start, end):
            if self.validbitmap[page] == True:
                self.read_page(page, cat)

    def write_page(self, lba_pagenum, garbage_collect_enable=True, cat='user'):
        """
        When write a page P:
        1. check if P's block is in device
            if no, take a new block from free block list and write all the pages
                until P, sequentially
            if yes, read all the valid pages from the block, modify it in memory,
                then write the pages sequentially back, until the last valid page
        """
        lba_block, lba_off = page_to_block_off(lba_pagenum)

        if self.blk_l2p.has_key(lba_block):
            is_new_block = False
            flash_block = self.blk_l2p[lba_block]
        else:
            is_new_block = True
            flash_block = self.pop_a_free_block()
            # put this new block to map
            self.blk_l2p[lba_block] = flash_block
            self.blk_p2l[flash_block] = lba_block

        flash_page = self.lba_page_to_flash_page(lba_pagenum)

        # now read all the valid pages from this block
        # it is possible this block has no valid pages if it is a new block
        start, end = block_to_page_range(flash_block)
        maxtowrite = 0
        for page in range(start, end):
            if self.validbitmap[page] == True:
                if page > maxtowrite:
                    maxtowrite = page
                cat = 'amplified'
                if page != flash_page:
                    # you don't need to read the page you will overwrite
                    self.read_page(page, cat)

        # modify pages in memory

        maxtowrite = max(maxtowrite, flash_page)

        # write all pages until the last valid
        if not is_new_block:
            # new block does not need erasure
            self.erase_block(flash_block, 'amplified')

        # set the flash_page as valid if it is not
        self.validate_flash_page(flash_page)

        for pagenum in range(start, maxtowrite+1):
            if self.validbitmap[pagenum] == True:
                # only write valid pages
                if pagenum == flash_page:
                    cat = 'user'
                else:
                    cat = 'amplified'
                flash.page_write(pagenum, cat)

    def block_invalid_ratio(self, blocknum):
        start, end = block_to_page_range(blocknum)
        return self.validbitmap[start:end].count(False) / float(config.flash_npage_per_block)

    def next_victim_block(self):
        "for block map, we can only garbage collect block with no valid pages at all"
        maxratio = -1
        maxblock = None

        for blocknum in self.usedblocks:
            invratio = self.block_invalid_ratio(blocknum)
            if invaratio == 1:
                return blocknum

        return None

    def used_to_free(self, blocknum):
        self.usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def free_to_used(self, blocknum):
        self.freeblocks.remove(blocknum)
        self.usedblocks.append(blocknum)

    def garbage_collect(self):
        """
        When needed, we recall all blocks with no valid page
        """
        recorder.debug('------------------------------------garbage collecting')

        block_to_clean = self.next_victim_block()
        while block_to_clean != None:
            self.erase_block(block_to_clean, 'amplified')

            # now remove the mappings
            lba_block = self.blk_p2l[block_to_clean]
            del self.blk_p2l[block_to_clean]
            del self.blk_l2p[lba_block]

            # move it to free list
            self.used_to_free(block_to_clean)

            block_to_clean = self.next_victim_block()


    def debug(self):
        self.show_map()
        recorder.debug( 'VALIDBITMAP', self.validbitmap)
        recorder.debug( 'FREEBLOCKS ', self.freeblocks)
        recorder.debug( 'USEDBLOCKS ', self.usedblocks)


ftl = Ftl(config.flash_page_size,
          config.flash_npage_per_block,
          config.flash_num_blocks)

def lba_read(pagenum):
    recorder.put('lba_read', pagenum, 'user')
    flash.page_read(pagenum, 'user')

def lba_write(pagenum):
    recorder.put('lba_write', pagenum, 'user')
    ftl.write_page(pagenum)

def lba_discard(pagenum):
    recorder.put('lba_discard ', pagenum, 'user')
    ftl.invalidate_lba_page(pagenum)


