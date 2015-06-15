import bitarray
from collections import deque

import ftlbuilder

class BlockMapFtl(ftlbuilder.FtlBuilder):
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

    def __init__(self, confobj, recorderobj, flashobj):
        # From parent:
        # self.conf = confobj
        # self.recorder = recorder

        # self.bitmap = FlashBitmap(self.conf.total_num_pages())
        super(BlockMapFtl, self).__init__(confobj, recorder, flash)
        print 'length...', self.bitmap.length()

        # initialize bitmap 1: valid, 0: invalid
        self.bitmap.setall(ftlbuilder.FlashBitmap.INVALID)
        print 'length...', self.bitmap.length()

        self.blk_l2p = {} # logical (LBA) block (multiple pages)
                          # to physical (flash) block
        self.blk_p2l = {}

        # this should be maitained a queue
        # later we may maitain it as a set, where we can pick a optimal
        # block as the next block for wear leveling
        # we can check self.freeblocks to see if we need do garbage collection
        self.freeblocks = deque(range(self.conf['flash_num_blocks']))
        self.usedblocks = []

        # trigger garbage collectionif the number of free blocks is below
        # the number below
        self.low_num_blocks = 0.5 * self.conf['flash_num_blocks']

    def lba_read(self, pagenum):
        self.recorder.put('lba_read', pagenum, 'user')
        self.flash.page_read(pagenum, 'user')

    def lba_write(self, pagenum):
        self.recorder.put('lba_write', pagenum, 'user')
        self.write_page(pagenum)

    def lba_discard(self, pagenum):
        self.recorder.put('lba_discard ', pagenum, 'user')
        self.invalidate_lba_page(pagenum)

    # basic operations
    def read_page(self, pagenum, cat):
        self.flash.page_read(pagenum, cat)

    def read_block(self, blocknum, cat):
        start, end = self.conf.block_to_page_range(blocknum)
        self.read_page_range(start, end, cat)

    def read_page_range(self, start, end, cat):
        for pagenum in range(start, end):
            self.flash.page_read(pagenum, cat)

    def program_block(self, blocknum, cat):
        start, end = self.conf.block_to_page_range(blocknum)
        for pagenum in range(start, end):
            self.flash.page_write(pagenum, cat)

    def program_page_range(self, start, end, cat):
        for pagenum in range(start, end):
            self.flash.page_write(pagenum, cat)

    def erase_block(self, blocknum, cat):
        self.flash.block_erase(blocknum, cat)

    def modify_page_in_ram(self, pagenum):
        "this is a dummy function"
        pass

    def pop_a_free_block(self):
        # take a block from free block queue
        # assert len(self.freeblocks) > 0, 'No free blocks in device!!!'
        if len(self.freeblocks) == 0:
            self.recorder.error('No free blocks in device!!!!')
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
        self.recorder.debug(self.blk_l2p)
        self.recorder.debug(self.blk_p2l)

    def lba_page_to_flash_page(self, lbapagenum):
        lba_block, lba_off = self.conf.page_to_block_off(lbapagenum)
        if self.blk_l2p.has_key(lba_block):
            flash_block = self.blk_l2p[lba_block]
            return self.conf.block_off_to_page(flash_block, lba_off)
        else:
            return None

    def flash_page_to_lba_page(self, flash_page):
        flash_block, flash_off = self.conf.page_to_block_off(flash_page)
        if self.blk_p2l.has_key(flash_block):
            lba_block = self.blk_p2l[flash_block]
            return self.conf.block_off_to_page(lba_block, flash_off)
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
            self.bitmap.invalidate_page(flash_page)
        else:
            self.recorder.warning('trying to invalidate a page not in block map')

    def read_valid_pages(self, blocknum, cat):
        start, end = self.conf.block_to_page_range(blocknum)
        for page in range(start, end):
            if self.bitmap.is_page_valid(page):
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
        lba_block, lba_off = self.conf.page_to_block_off(lba_pagenum)

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
        start, end = self.conf.block_to_page_range(flash_block)
        maxtowrite = 0
        for page in range(start, end):
            if self.bitmap.is_page_valid(page):
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
        self.bitmap.validate_page(flash_page)

        for pagenum in range(start, maxtowrite+1):
            if self.bitmap.is_page_valid(pagenum):
                # only write valid pages
                if pagenum == flash_page:
                    cat = 'user'
                else:
                    cat = 'amplified'
                self.flash.page_write(pagenum, cat)

        if len(self.freeblocks) < self.low_num_blocks:
            self.garbage_collect()

    def next_victim_block(self):
        "for block map, we can only garbage collect block with no valid pages at all"
        maxratio = -1
        maxblock = None

        for blocknum in self.usedblocks:
            invratio = self.bitmap.block_invalid_ratio(blocknum)
            if invratio == 1:
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
        self.recorder.debug('------------------------------------garbage collecting')

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

        self.recorder.debug('===================================garbage collecting ends')

    def debug(self):
        self.show_map()
        self.recorder.debug( 'VALIDBITMAP', self.bitmap)
        self.recorder.debug( 'FREEBLOCKS ', self.freeblocks)
        self.recorder.debug( 'USEDBLOCKS ', self.usedblocks)



