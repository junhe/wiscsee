import flash
import recorder
import config
from common import *
import bitarray
from collections import deque

class Ftl:
    """
    Write a page (not program a page). In this case, every block is log block.
    So there is an end log (pointer).
    1. append the page to the log
    2. mark new page as valid
    3. mark the old page as invalid

    when the number of free blocks drops below a threshold, we need to start GC.
    while free blocks < threshold:
        - pick a victim block (greedy? Hotness? random for prototype?)
        - copy valid pages in victim block and append them to appendpoint
        - erase victim block
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

        # setup the mappings
        # physical to logical mapping can be convenient when we need to
        # copy valid pages in a block to a new new (we need to know the
        # logical page number of a physical page we see in the victim
        # block.
        self.l2p = {} # logical (LBA) page to physical (flash) page
        self.p2l = {} # physical (flash) page to logical (LBA) page

        self.log_end_pagenum = -1 # the page number of the last write

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
        # if a page is invalid, we don't need to keep its mappings
        if self.p2l.has_key(pagenum):
            del self.p2l[pagenum]

            # we cannot delete l2p[lba] because it may point to a
            # valid page
            # if self.l2p.has_key(lba):
                # del self.l2p[lba]

    def validate_flash_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = True

    def invalidate_flash_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = False

    # basic operations
    def read_page(self, pagenum):
        flash.page_read(pagenum)

    def read_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        for pagenum in range(start, end):
            flash.page_read(pagenum)

    def program_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        for pagenum in range(start, end):
            flash.page_write(pagenum)

    def erase_block(self, blocknum):
        flash.block_erase(blocknum)

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
        print self.l2p
        print self.p2l

    def invalidate_lba_page(self, lbapagenum):
        "invalidate bitmap and remove the mapping"
        if self.l2p.has_key(lbapagenum):
            flashpagenum = self.l2p[lbapagenum]
            assert self.validbitmap[flashpagenum], 'WTF, in map but not valid?'
            self.invalidate_flash_page(flashpagenum)
            del self.l2p[lbapagenum]
            # del self.p2l[flashpagenum]
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

        nextpage = (curpage + 1)%total_num_pages()
        nextblock, nextoff = page_to_block_off(nextpage)

        if curblock == nextblock:
            return nextpage
        else:
            block = self.pop_a_free_block()
            start, end = block_to_page_range(block)
            return start

    def write_page(self, lba_pagenum, garbage_collect_enable=True):
        """
        1. find the next flash page to write, write it
        2. if exists, find and invalidate the old flash page of the LBA page
        3. update the mapping tables to reflect the new mapping
        """
        toflashpage = self.next_page_to_program()
        recorder.debug('Writing LBA {} to {}'.format(lba_pagenum, toflashpage))
        assert self.validbitmap[toflashpage] == False
        flash.page_write(toflashpage)
        self.log_end_pagenum = toflashpage

        # if there is a flash page for this LBA, invalidate the flash page
        if self.l2p.has_key(lba_pagenum):
            oldflashpage = self.l2p[lba_pagenum]
            self.invalidate_flash_page(oldflashpage)

        # These operations will make p2l larger than l2p, because l2p is
        # overwritten. p2l may be adding.
        self.l2p[lba_pagenum] = toflashpage
        self.p2l[toflashpage] = lba_pagenum
        self.validate_flash_page(toflashpage)

        # do garbage collection if necessary
        if garbage_collect_enable == True and \
                len(self.freeblocks) < self.low_num_blocks:
            self.garbage_collect()

    def block_invalid_ratio(self, blocknum):
        start, end = block_to_page_range(blocknum)
        return self.validbitmap[start:end].count(False) / float(config.flash_npage_per_block)

    def next_victim_block(self):
        """
        Let me use FIFO in the prototype first
        Note that we DO NOT remove victim block from used list here
        You will need to remove it after you have erased the block
        """
        # if len(self.usedblocks) > 0:
            # return self.usedblocks[0]
        # else:
            # return None

        # print 'for next victim', self.validbitmap

        # use stupid for the prototype
        maxratio = -1
        maxblock = None
        # we don't want usedblocks[-1] because it is the one in use, newly popped block
        # is appended to the used block list
        for blocknum in self.usedblocks[0:-1]:
            invratio = self.block_invalid_ratio(blocknum)
            if invratio > maxratio:
                maxblock = blocknum
                maxratio = invratio

        if maxratio == 0:
            return None

        return maxblock

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
                lba = self.p2l[page]
                self.write_page(lba, garbage_collect_enable=False)


    def used_to_free(self, blocknum):
        self.usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def free_to_used(self, blocknum):
        self.freeblocks.remove(blocknum)
        self.usedblocks.append(blocknum)

    def garbage_collect(self):
        """
        this function is called when len(self.freeblocks) is
        smaller than a threshold.
        """
        recorder.debug('------------------------------------garbage collecting')

        lastfree = len(self.freeblocks)
        cnt = 0
        while len(self.freeblocks) < self.low_num_blocks*2:
            # while we still can find victim blocks
            # 1. read valid pages from the victim block
            # 2. write the valid pages as if the are new
            # 3. erase the victimblock

            victimblock = self.next_victim_block()
            if victimblock == None:
                # if next_victim_block() return None, it means
                # no block can be a victim
                break
            print 'next victimblock:', victimblock, \
                    'invaratio', self.block_invalid_ratio(victimblock)
            # self.debug()

            self.move_valid_pages(victimblock)
            self.erase_block(victimblock)
            self.used_to_free(victimblock)

            print 'freeblocks', self.freeblocks
            print 'usedblocks', self.usedblocks

            cnt += 1
            if cnt % 10 == 0:
                # time to check
                if len(self.freeblocks) >= lastfree:
                    # Not making progress
                    break
                else:
                    lastfree = len(self.freeblocks)

        recorder.debug('==================================garbage collecting ends')

    def debug(self):
        self.show_map()
        print 'VALIDBITMAP', self.validbitmap
        print 'FREEBLOCKS ', self.freeblocks
        print 'USEDBLOCKS ', self.usedblocks


ftl = Ftl(config.flash_page_size,
          config.flash_npage_per_block,
          config.flash_num_blocks)

def lba_read(pagenum):
    recorder.debug('lba_read '+str(pagenum))
    flash.page_read(pagenum)

def lba_write(pagenum):
    recorder.debug('lba_write '+str(pagenum))
    ftl.write_page(pagenum)

def lba_discard(pagenum):
    recorder.debug('lba_discard '+str(pagenum))
    ftl.invalidate_lba_page(pagenum)


