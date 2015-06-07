from collections import deque

import bitarray

from common import *
import config
import flash
import recorder

class Ftl:
    """
    There are two types of blocks: log block and data block. I think we need
    to maitain two sets of free blocks and used blocks

    Because of the two types of blocks, we also need to sets of mappings:
        - page map
        - block map

    validbitmap seems to be able to work with page block and data block

    Garbage collection: when the number of log blocks is below water mark or
    the number of data blocks is below a water mark, we need to do garbage
    collection. We need to merge.
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

        self.log_page_l2p = {}
        self.log_page_p2l = {}

        self.data_blk_l2p = {}
        self.data_blk_p2l = {}

        self.log_end_pagenum = -1 # the page number of the last write

        # initialize free list
        self.freeblocks = deque(range(self.flash_num_blocks))
        # initialize usedblocks
        self.log_usedblocks = []
        self.data_usedblocks = []

        # self.log_low_num_blocks = int(config.low_log_block_ratio
            # * self.flash_num_blocks)
        # self.data_low_num_blocks = int(config.low_data_block_ratio
            # * self.flash_num_blocks)
        self.log_high_num_blocks = int(config.high_log_block_ratio
            * self.flash_num_blocks)
        self.data_high_num_blocks = int(config.high_data_block_ratio
            * self.flash_num_blocks)

        recorder.debug('log_high_num_blocks', self.log_high_num_blocks)
        recorder.debug('data_high_num_blocks', self.data_high_num_blocks)

    # bitmap operations
    def validate_flash_page(self, pagenum):
        "use this function to wrap the operation, "\
        "in case I change bitmap module later"
        self.validbitmap[pagenum] = True

    def invalidate_flash_page(self, pagenum):
        self.validbitmap[pagenum] = False

    def validate_flash_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = True

    def invalidate_flash_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = False

    # mapping operations
    def add_data_blk_mapping(self, lba_block, flash_block):
        """
        we don't want l2p and p2l to be asymmetric, so we assert
        The assertion will make sure that we increase both mappings
        by one after the function.
        """
        assert not self.data_blk_l2p.has_key(lba_block)
        assert not self.data_blk_p2l.has_key(flash_block)

        self.data_blk_l2p[lba_block] = flash_block
        self.data_blk_p2l[flash_block] = lba_block

    def remove_data_blk_mapping_by_lba(self, lba_block):
        "The mapping must exist"
        flash_block = self.data_blk_l2p[lba_block]
        del self.data_blk_l2p[lba_block]
        del self.data_blk_p2l[flash_block]

    def remove_data_blk_mapping_by_flash(self, flash_block):
        "The mapping must exist"
        lba_block = self.data_blk_p2l[flash_block]
        del self.data_blk_l2p[lba_block]
        del self.data_blk_p2l[flash_block]

    def add_log_page_mapping(self, lba_page, flash_page):
        assert not self.log_page_l2p.has_key(lba_page)
        assert not self.log_page_p2l.has_key(flash_page)

        self.log_page_l2p[lba_page] = flash_page
        self.log_page_p2l[flash_page] = lba_page

    def remove_log_page_mapping_by_lba(self, lba_page):
        "The mapping must exist"
        flash_page = self.log_page_l2p[lba_page]
        del self.log_page_l2p[lba_page]
        del self.log_page_p2l[flash_page]

    def remove_log_page_mapping_by_flash(self, flash_page):
        "The mapping must exist"
        lba_page = self.log_page_p2l[flash_page]
        del self.log_page_l2p[lba_page]
        del self.log_page_p2l[flash_page]

    # basic operations
    def read_page(self, pagenum, cat):
        flash.page_read(pagenum, cat)

    def read_block(self, blocknum, cat):
        start, end = block_to_page_range(blocknum)
        for pagenum in range(start, end):
            flash.page_read(pagenum, cat)

    def program_block(self, blocknum, cat):
        start, end = block_to_page_range(blocknum)
        for pagenum in range(start, end):
            flash.page_write(pagenum, cat)

    def erase_block(self, blocknum, cat):
        flash.block_erase(blocknum, cat)

    def modify_page_in_ram(self, pagenum):
        "this is a dummy function"
        pass

    def pop_a_free_block(self):
        if self.freeblocks:
            blocknum = self.freeblocks.popleft()
        else:
            # nobody has free block
            recorder.error('No free blocks in device!!!!')
            # TODO: maybe try garbage collecting here
            exit(1)

        return blocknum

    def pop_a_free_block_to_log(self):
        blocknum = self.pop_a_free_block()
        self.log_usedblocks.append(blocknum)
        return blocknum

    def pop_a_free_block_to_data(self):
        blocknum = self.pop_a_free_block()
        self.data_usedblocks.append(blocknum)
        return blocknum

    def invalidate_lba_page(self, lbapagenum):
        "invalidate bitmap and remove the mapping"
        lba_block, lba_off = page_to_block_off(lbapagenum)

        if self.log_page_l2p.has_key(lbapagenum):
            # in log block
            flashpagenum = self.log_page_l2p[lbapagenum]
            assert self.validbitmap[flashpagenum], 'WTF, in map but not valid?'
            self.invalidate_flash_page(flashpagenum)
            remove_log_page_mapping_by_lba(lbapagenum)
        elif self.data_blk_l2p.has_key(lba_block):
            # in data block
            flashpagenum = self.lba_page_to_flash_page(lbapagenum)
            assert self.validbitmap[flashpagenum], 'WTF, in map but not valid?'
            self.invalidate_flash_page(flashpagenum)
        else:
            recorder.warning('trying to invalidate a page not in page map')

    def next_page_to_program(self):
        """
        it finds out the next available page to write,
        usually it is the page after log_end_pagenum.

        If next=log_end_pagenum + 1 is in the same block with
        log_end_pagenum, simply return log_end_pagenum + 1
        If next=log_end_pagenum + 1 is out of the block of
        log_end_pagenum, we need to pick a new block from self.freeblocks
        """
        curpage = self.log_end_pagenum
        curblock, curoff = page_to_block_off(curpage)

        nextpage = (curpage + 1) % total_num_pages()
        nextblock, nextoff = page_to_block_off(nextpage)

        if curblock == nextblock:
            return nextpage
        else:
            block = self.pop_a_free_block_to_log()
            start, end = block_to_page_range(block)
            return start

    def lba_page_to_flash_page(self, lba_pagenum):
        lba_block, lba_off = page_to_block_off(lba_pagenum)

        if self.log_page_l2p.has_key(lba_pagenum):
            return self.log_page_l2p[lba_pagenum]
        elif self.data_blk_l2p.has_key(lba_block):
            flash_block = self.data_blk_l2p[lba_block]
            return block_off_to_page(flash_block, lba_off)
        else:
            return None

    def write_page(self, lba_pagenum, garbage_collect_enable, cat):
        """
        to write a page:
            1. find a new page in log blocks by next_page_to_program()
            2. write the data to the new flash page
            3. update the log_end_pagenum
            4. check if the lba page is in log block or data block
                if the page is in log block,
                    invalidate the old flash page
                    update the log_page_p2l and log_page_l2p
                if the page is in data block
                    invalidate the old flash page
                    you don't need to update data_blk_p2l and data_blk_l2p
                     because the mapping is still meaningful for other
                     valid pages in the block
            5. check if we need to do garbage collect. When number of log blocks
               is low, we need to merge log blocks to data block. When number of
               data blocks is low, we need to data blocks with no valid pages

               to merge log blocks (to increase the number of log blocks):
                    1. pick a victim by greedy within log blocks
                    2. do partial merge, switch merge, or full merge
                        - switch merge: if the victim has exact pages of
                        a block, remove the mapping of all pages from log map,
                        add a mapping in block map
                        - partial merge: the beginning of a log block has pages
                        of corresponding logical block, so all you need to do
                        is to move the rest of the pages from somewhere else.
                        - full merge: move each page in the block to be with
                        other pages in the same block

                to clean data blocks, find the ones without valid page and erase
                them and remove their mappings
        """
        lba_block, lba_off = page_to_block_off(lba_pagenum)

        toflashpage = self.next_page_to_program()
        recorder.debug('Writing LBA {} to {}'.format(lba_pagenum, toflashpage))
        assert self.validbitmap[toflashpage] == False
        flash.page_write(toflashpage, cat)
        self.log_end_pagenum = toflashpage

        if self.log_page_l2p.has_key(lba_pagenum):
            oldflashpage = self.log_page_l2p[lba_pagenum]
            self.invalidate_flash_page(oldflashpage)
            self.remove_log_page_mapping_by_lba(lba_pagenum)
        elif self.data_blk_l2p.has_key(lba_block):
            oldflashpage = self.lba_page_to_flash_page(lba_pagenum)
            self.invalidate_flash_page(oldflashpage)

        self.add_log_page_mapping(lba_page=lba_pagenum, flash_page=toflashpage)
        self.validate_flash_page(toflashpage)

        # do garbage collection if necessary
        if garbage_collect_enable and \
            (len(self.log_usedblocks) >= self.log_high_num_blocks or \
            len(self.data_usedblocks) >= self.data_high_num_blocks):
            self.garbage_collect()

    def garbage_collect(self):
        recorder.debug('************************************************************')
        recorder.debug('****************** start ***********************************')
        self.garbage_collect_log_blocks()
        self.garbage_collect_merge()
        self.garbage_collect_data_blocks()
        self.debug()
        recorder.debug('******************** end *********************************')
        recorder.debug('**********************************************************')

    def block_invalid_ratio(self, blocknum):
        start, end = block_to_page_range(blocknum)
        return self.validbitmap[start:end].count(False) / float(config.flash_npage_per_block)

    def next_victim_log_block_to_merge(self):
        # use stupid for the prototype
        maxratio = -1
        maxblock = None
        # we don't want usedblocks[-1] because it is the one in use, newly popped block
        # is appended to the used block list
        for blocknum in self.log_usedblocks[0:-1]:
            invratio = self.block_invalid_ratio(blocknum)
            if invratio > maxratio:
                maxblock = blocknum
                maxratio = invratio

        if maxblock == None:
            recorder.debug("no block in log_usedblocks[]")

        return maxblock

    def next_victim_log_block(self):
        # Greedy algorithm
        maxratio = -1
        maxblock = None
        # we don't want usedblocks[-1] because it is the one in use, newly popped block
        # is appended to the used block list
        for blocknum in self.log_usedblocks[0:-1]:
            invratio = self.block_invalid_ratio(blocknum)
            if invratio > maxratio:
                maxblock = blocknum
                maxratio = invratio

        if maxratio == 0:
            recorder.debug("Cannot find victimblock maxratio is", maxratio)
            return None

        if maxblock == None:
            recorder.debug("no block in usedblocks[]")

        return maxblock

    def next_victim_data_block(self):
        "for block map, we can only garbage collect block with no valid pages at all"
        maxratio = -1
        maxblock = None

        for blocknum in self.data_usedblocks:
            invratio = self.block_invalid_ratio(blocknum)
            if invratio == 1:
                return blocknum

        return None

    def move_valid_pages(self, blocknum):
        # this function read the valid pages in blocknum block and
        # write it as new pages. This is part of the garbage collection
        # process
        # note that this function does not erase this block

        # note *end* is not in block blocknum
        start, end = block_to_page_range(blocknum)

        # The loop below will invalidate all pages in this block
        for page in range(start, end):
            if self.validbitmap[page] == True:
                # lba = self.p2l[page]
                lba = self.flash_page_to_lba_page(page)
                self.write_page(lba, garbage_collect_enable=False, cat='amplified')

    def is_switch_mergable(self, flash_blocknum):
        """
        This is only for log block
        We define mergable to be that all pages in the block is valid and
        exactly corresponds to the lba block
        """
        flash_pg_start, flash_pg_end = block_to_page_range(flash_blocknum)

        for flash_pg in range(flash_pg_start, flash_pg_end):
            if self.validbitmap[flash_pg] == False:
                return False
            lba_pg = self.log_page_p2l[flash_pg]
            if lba_pg % self.npages_per_block != \
                flash_pg % self.npages_per_block:
                return False

        return True

    def flash_page_to_lba_page(self, flash_page):
        flash_block, flash_off = page_to_block_off(flash_page)

        if self.log_page_p2l.has_key(flash_page):
            return self.log_page_p2l[flash_page]
        elif self.data_blk_p2l.has_key[flash_block]:
            lba_block = self.data_blk_p2l[flash_block]
            return block_off_to_page(lba_block, flash_off)
        else:
            recorder.error("Cannot find flash page in any mapping table")
            exit(1)

    def switch_merge(self, flash_blocknum):
        """
        Before calling, you need to make sure that flash_block is switch mergable
        At beginning, the pages in flash_blocknum have mapping in log map
        you want to remove those and add block mapping in block map
        """
        recorder.debug("I am in switch_merge()-~~~~~~~~~~-")
        flash_pg_start, flash_pg_end = block_to_page_range(flash_blocknum)
        lba_pg_start = self.flash_page_to_lba_page(flash_pg_start)
        lba_block, lba_off = page_to_block_off(lba_pg_start)

        # add data block mapping
        self.add_data_blk_mapping(lba_block=lba_block,
            flash_block=flash_blocknum)

        # removing log page mapping
        for pg in range(flash_pg_start, flash_pg_end):
            if self.validbitmap[pg] == True:
                self.remove_log_page_mapping_by_flash(pg)

        # valid bitmap does not change

        # move the block from log blocks to data blocks
        self.log_usedblocks.remove(flash_blocknum)
        self.data_usedblocks.append(flash_blocknum)

        recorder.debug('SWITCH MERGE IS DONE')

    def aggregate_lba_block(self, lba_block, target_flash_block):
        """
        Given a lba block number, this function finds all its pages on flash,
        read them to memory, and write them to flash_block. flash_block has to
        be erased and writable.
        """
        recorder.debug2('In aggregate_lba_block from lba_block', lba_block,
                'to', target_flash_block)
        lba_start, lba_end = block_to_page_range(lba_block)
        moved = False
        for lba_page in range(lba_start, lba_end):
            page_off = lba_page % self.npages_per_block
            flash_page = self.lba_page_to_flash_page(lba_page)
            recorder.debug2('trying to move lba_page', lba_page,
                    '(flash_page:', flash_page, ')')

            if flash_page != None:
                # mapping exists (this lba page is on device)
                moved = True


                flash.page_read(flash_page, 'amplified')
                self.invalidate_flash_page(flash_page)

                target_page = block_off_to_page(target_flash_block, page_off)
                flash.page_write(target_page, 'amplified')
                self.validate_flash_page(target_page)

                if self.log_page_l2p.has_key(lba_page):
                    # handle the page mapping case
                    # you only need to delete the page mapping because
                    # later we will establish block mapping
                    self.remove_log_page_mapping_by_lba(lba_page)

                recorder.debug2('move lba', lba_page, '(flash:', flash_page,
                        ') to flash', target_page)

        # Now all pages of lba_block is in target_flash_block
        # we now need to handle the mappings
        if moved:
            if self.data_blk_l2p.has_key(lba_block):
                self.debug2()
                # the lba block was in the mapping
                flash_block = self.data_blk_l2p[lba_block]
                recorder.debug2('lba_block', lba_block,
                                'flash_block', flash_block)
                self.remove_data_blk_mapping_by_lba(lba_block)
                self.add_data_blk_mapping(lba_block=lba_block,
                    flash_block=target_flash_block)
            else:
                self.add_data_blk_mapping(lba_block=lba_block,
                    flash_block=target_flash_block)

        self.debug()
        recorder.debug('End aggregate_lba_block:')

    def full_merge(self, flash_blocknum):
        """
        For each valid flash page, we find all other pages in the same lba
        block with it and move them to a new flash block.
        We need to add mapping for the new flash block
        """
        recorder.debug('I am in full_merge()!!!!!!!!~~~~~~~~~~-!')

        flash_start, flash_end = block_to_page_range(flash_blocknum)

        # find all the lba blocks of the pages in flash_blocknum
        lbablocks = set()
        for flash_pg in range(flash_start, flash_end):
            if self.validbitmap[flash_pg] == True:
                lba_pg = self.flash_page_to_lba_page(flash_pg)
                lba_blk, off = page_to_block_off(lba_pg)
                lbablocks.add(lba_blk)

        # aggregate all the lba blocks
        for lba_block in lbablocks:
            new_data_block = self.pop_a_free_block_to_data()
            self.aggregate_lba_block(lba_block, new_data_block)

        # Now block flash_blocknum should have no valid pages
        # we can now erase it and put it to free list
        # flash_blocknum is a log block, and all its page mapping
        # have been handled above, so we don't need to worry about
        # page mapping.
        self.erase_block(flash_blocknum, 'amplified')
        self.log_usedblocks.remove(flash_blocknum)
        self.freeblocks.append(flash_blocknum)

    def is_partial_mergable(self, flash_blocknum):
        """
        A block is partially mergable if the first k pages of it is the first k
        pages of a lba block. And the rest of the block has not been written.
        Note that right now I cannot check if a page has been written or not,
        because a page can be written and invalidated. You cannot find out if
        a page has been written or not by checking validbitmap.
        """
        return False

    def merge_log_block(self, flash_blocknum):
        """
        try switch merge
        try partial merge
        try full merge
        """
        recorder.debug('I am in merge_log_block()')

        if self.is_switch_mergable(flash_blocknum):
            self.switch_merge(flash_blocknum)
        elif self.is_partial_mergable(flash_blocknum):
            # let us do full mergen even the block is partial mergable for now
            # but this branch will never be entered because
            # is_partial_mergable() always return false
            self.full_merge(flash_blocknum)
        else:
            self.full_merge(flash_blocknum)

    def garbage_collect_log_blocks(self):
        """
        There will be three types of garbage collection:
            1. garbage collection within log blocks: this is the same as the
            garbage collection in page mapping. Data blocks are not involved.
                - garbage_collect_log_blocks()
            2. merging log blocks to be data blocks
                - merge_log_block()
            3. clean data blocks: this removes data blocks without valid pages
                - garbage_collect_data_blocks()
        """
        recorder.debug('-----------garbage_collect_log_blocks-------------------------garbage collecting')

        lastused = len(self.log_usedblocks)
        cnt = 0
        while len(self.log_usedblocks) >= self.log_high_num_blocks:
            # used too many log blocks, need to garbage collect some to
            # free some, hopefully
            victimblock = self.next_victim_log_block()
            if victimblock == None:
                # if next_victim_block() return None, it means
                # no block can be a victim
                recorder.debug( self.validbitmap )
                recorder.debug('Cannot find a victim block')
                break
            recorder.debug( 'next victimblock:', victimblock,
                    'invaratio', self.block_invalid_ratio(victimblock))
            recorder.debug( self.validbitmap )

            self.move_valid_pages(victimblock)
            #block erasure is always counted as amplified
            self.erase_block(victimblock, 'amplified')

            # move from used to free
            self.log_usedblocks.remove(victimblock)
            self.freeblocks.append(victimblock)

            cnt += 1
            if cnt % 10 == 0:
                # time to check
                if len(self.log_usedblocks) >= lastused:
                    # Not making progress
                    recorder.debug( self.validbitmap )
                    recorder.debug('GC is not making progress! End GC')
                    break
                lastused = len(self.log_usedblocks)

        recorder.debug('============garbage_collect_log_blocks======================garbage collecting ends')

    def garbage_collect_merge(self):
        recorder.debug('-----------garbage_collect_merge()-------------------------garbage collecting')
        self.debug()
        while len(self.log_usedblocks) >= self.log_high_num_blocks:
            victimblock = self.next_victim_log_block_to_merge()
            if victimblock == None:
                recorder.debug( self.validbitmap )
                recorder.debug('Cannot find a victim block')
                break
            recorder.debug( 'next victimblock:', victimblock,
                    'invaratio', self.block_invalid_ratio(victimblock))
            recorder.debug( self.validbitmap )

            self.merge_log_block(victimblock)

            #
        recorder.debug('============garbage_collect_merge()======================garbage collecting ends')

    def garbage_collect_data_blocks(self):
        """
        When needed, we recall all blocks with no valid page
        """
        recorder.debug('-----------------garbage_collect_data_blocks-------------------garbage collecting')

        block_to_clean = self.next_victim_data_block()
        while block_to_clean != None:
            self.erase_block(block_to_clean, 'amplified')

            # now remove the mappings
            # some blocks may be used by has no mapping
            if self.data_blk_p2l.has_key(block_to_clean):
                self.remove_data_blk_mapping_by_flash(block_to_clean)

            # move it to free list
            self.data_usedblocks.remove(block_to_clean)
            self.freeblocks.append(block_to_clean)

            block_to_clean = self.next_victim_data_block()

        recorder.debug('=============garbage_collect_data_blocks======================garbage collecting ends')

    def debug(self):
        recorder.debug('log_page_l2p', self.log_page_l2p)
        recorder.debug('log_page_p2l', self.log_page_p2l)

        recorder.debug('data_blk_l2p', self.data_blk_l2p)
        recorder.debug('data_blk_p2l', self.data_blk_p2l)

        recorder.debug('* VALIDBITMAP', self.validbitmap)
        recorder.debug('* freeblocks ', self.freeblocks)
        recorder.debug('* log_usedblocks ', self.log_usedblocks)
        recorder.debug('* data_usedblocks', self.data_usedblocks)

    def debug2(self):
        recorder.debug2('log_page_l2p', self.log_page_l2p)
        recorder.debug2('log_page_p2l', self.log_page_p2l)

        recorder.debug2('data_blk_l2p', self.data_blk_l2p)
        recorder.debug2('data_blk_p2l', self.data_blk_p2l)

        recorder.debug2('* VALIDBITMAP', self.validbitmap)
        recorder.debug2('* freeblocks ', self.freeblocks)
        recorder.debug2('* log_usedblocks ', self.log_usedblocks)
        recorder.debug2('* data_usedblocks', self.data_usedblocks)

    def show_map(self):
        recorder.debug('log_page_l2p', self.log_page_l2p)
        recorder.debug('log_page_p2l', self.log_page_p2l)

        recorder.debug('data_blk_l2p', self.data_blk_l2p)
        recorder.debug('data_blk_p2l', self.data_blk_p2l)


ftl = Ftl(config.flash_page_size,
          config.flash_npage_per_block,
          config.flash_num_blocks)

def lba_read(pagenum):
    recorder.put('lba_read', pagenum, 'user')
    flash.page_read(pagenum, 'user')

def lba_write(pagenum):
    recorder.put('lba_write', pagenum, 'user')
    ftl.write_page(pagenum, garbage_collect_enable=True, cat='user')

def lba_discard(pagenum):
    recorder.put('lba_discard ', pagenum, 'user')
    ftl.invalidate_lba_page(pagenum)



