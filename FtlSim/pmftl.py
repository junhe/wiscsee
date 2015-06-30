import bitarray
from collections import deque

import ftlbuilder

class GarbageCollectionDecider(object):
    """
    it is a queue maintaining GC activity in the past a few
    writes.
    old ......... new
    [True, False, True]
    If it is true, means GC is triggered for a write.

    it also tells you whether you should do GC.
    """
    def __init__(self, threshold, pause_cnt, freeblocks, low_num_blocks):
        """
        threshold: pause GC when threshold writes in the past
            has triggerred GC.
        pause_cnt: pause GC for this many writes

        This class should be used like this:

        write_page():
            do_gc = should_do_gc()
            if do_gc == True:
                gc()
            add_gc_activity(do_gc)


        """
        self.threshold = int(threshold)
        self.pause_cnt = pause_cnt
        self.queue_size = threshold
        self.freeblocks = freeblocks # Python passes in as reference
        self.low_num_blocks = low_num_blocks
        self.pause_remaining = 0

        self.gc_activity = []

    def should_do_gc(self):
        """
        We should NOT do gc when:
        1. gc has been triggerred for the past threshold user writes.
        2. number of freeblocks is more then low_num_blocks
        3. pause_remaining > 0
        """
        if len(self.freeblocks) > self.low_num_blocks or \
            self.pause_remaining > 0:
            return False

        return True

    def add_gc_activity(self, gc_bool):
        """
        This function should be called for every write.
        This also dequeues if the queue is long
        """
        self.pause_cnt -= 1

        self.gc_activity.append(gc_bool)
        while len(self.gc_activity) > self.queue_size:
            # no need to check size here, the while check already did
            del self.gc_activity[0]

        if self.pause_remaining <= 0 and \
            all(self.gc_activity[-self.threshold:]):
            # not in pausing status and found we have been doing
            # too many garbage collecting
            self.pause_remaining = self.pause_cnt
            # print 'reset pause_remaining'


class PageMapFtl(ftlbuilder.FtlBuilder):
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
    def __init__(self, confobj, recorder, flash):
        # From parent:
        # self.conf = confobj
        # self.recorder = recorder
        # self.flash = flash
        super(PageMapFtl, self).__init__(confobj, recorder, flash)

        # initialize bitmap 1: valid, 0: invalid
        self.bitmap.initialize()

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
        self.freeblocks = deque(range(self.conf['flash_num_blocks']))
        self.usedblocks = []

        # trigger garbage collection if the number of free blocks is below
        # the number below
        self.low_num_blocks = 0.5 * self.conf['flash_num_blocks']
        self.gc_decider = GarbageCollectionDecider(threshold = 3,
            pause_cnt = 1000, freeblocks = self.freeblocks,
            low_num_blocks = self.low_num_blocks)

    def lba_read(self, pagenum):
        self.recorder.put('lba_read', pagenum, 'user')
        self.flash.page_read(pagenum, 'user')

    def lba_write(self, pagenum):
        self.recorder.put('lba_write', pagenum, 'user')
        self.write_page(pagenum)

    def lba_discard(self, pagenum):
        self.recorder.put('lba_discard', pagenum, 'user')
        self.invalidate_lba_page(pagenum)

    # basic operations
    def read_page(self, pagenum, cat):
        self.flash.page_read(pagenum, cat)

    def read_block(self, blocknum, cat):
        start, end = self.conf.block_to_page_range(blocknum)
        for pagenum in range(start, end):
            self.flash.page_read(pagenum, cat)

    def program_block(self, blocknum, cat):
        start, end = self.conf.block_to_page_range(blocknum)
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
            raise RuntimeError('No free blocks in device!!!!')

        blocknum = self.freeblocks.popleft()
        # now the block is busy, we put it to the used list.
        # a block is either in the used list or the free list
        self.usedblocks.append(blocknum)
        return blocknum

    def append_a_free_block(self, blocknum):
        "Note that it removes block num from the used list"
        self.usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def invalidate_lba_page(self, lbapagenum):
        "invalidate bitmap and remove the mapping"
        if self.l2p.has_key(lbapagenum):
            flashpagenum = self.l2p[lbapagenum]
            assert self.bitmap.is_page_valid(flashpagenum), 'WTF, in map but not valid?'
            self.bitmap.invalidate_page(flashpagenum)
            del self.l2p[lbapagenum]
            # del self.p2l[flashpagenum]
        else:
            self.recorder.warning('trying to invalidate a page not in page map')

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
        curblock, curoff = self.conf.page_to_block_off(curpage)

        nextpage = (curpage + 1) % self.conf.total_num_pages()
        nextblock, nextoff = self.conf.page_to_block_off(nextpage)

        if curblock == nextblock:
            return nextpage
        else:
            block = self.pop_a_free_block()
            start, end = self.conf.block_to_page_range(block)
            return start

    def write_page(self, lba_pagenum, garbage_collect_enable=True, cat='user'):
        """
        1. find the next flash page to write, write it
        2. if exists, find and invalidate the old flash page of the LBA page
        3. update the mapping tables to reflect the new mapping
        """
        toflashpage = self.next_page_to_program()
        self.recorder.debug('Writing LBA {} to {}'.format(lba_pagenum,
            toflashpage))
        assert self.bitmap.is_page_valid(toflashpage) == False
        self.flash.page_write(toflashpage, cat)
        self.log_end_pagenum = toflashpage

        # if there is a flash page for this LBA, invalidate the flash page
        if self.l2p.has_key(lba_pagenum):
            oldflashpage = self.l2p[lba_pagenum]
            self.bitmap.invalidate_page(oldflashpage)

        # These operations will make p2l larger than l2p, because l2p is
        # overwritten. p2l may be adding.
        self.l2p[lba_pagenum] = toflashpage
        self.p2l[toflashpage] = lba_pagenum
        self.bitmap.validate_page(toflashpage)

        # do garbage collection if necessary
        do_gc = self.gc_decider.should_do_gc()
        if garbage_collect_enable == True and do_gc == True:
            self.garbage_collect()
        self.gc_decider.add_gc_activity(do_gc)

    def block_invalid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return self.bitmap.bitmap[start:end].count(False) / \
            float(self.conf['flash_npage_per_block'])

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
            self.recorder.debug("Cannot find victimblock maxratio is", maxratio)
            return None

        if maxblock == None:
            self.recorder.debug("no block in usedblocks[]")

        return maxblock

    def move_valid_pages(self, blocknum):
        # this function read the valid pages in blocknum block and
        # write it as new pages. This is part of the garbage collection
        # process
        # note that this function does not erase this block

        # note *end* is not in block blocknum
        start, end = self.conf.block_to_page_range(blocknum)

        # The loop below will invalidate all pages in this block
        for page in range(start, end):
            if self.bitmap.is_page_valid(page) == True:
                lba = self.p2l[page]
                self.write_page(lba, garbage_collect_enable=False,
                    cat='amplified')

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
        self.recorder.debug('------------------------------------garbage collecting')

        lastfree = len(self.freeblocks)
        cnt = 0
        while len(self.freeblocks) < self.low_num_blocks * 1.5:
        # while True:
            # while we still can find victim blocks
            # 1. read valid pages from the victim block
            # 2. write the valid pages as if the are new
            # 3. erase the victimblock

            victimblock = self.next_victim_block()
            if victimblock == None:
                # if next_victim_block() return None, it means
                # no block can be a victim
                self.recorder.debug( self.bitmap.bitmap )
                self.recorder.debug('Cannot find a victim block')
                break
            self.recorder.debug( 'next victimblock:', victimblock,
                'invaratio', self.block_invalid_ratio(victimblock))
            self.recorder.debug( self.bitmap.bitmap )
            # self.debug()

            self.move_valid_pages(victimblock)
            # block erasure is always counted as amplified
            self.erase_block(victimblock, 'amplified')
            self.used_to_free(victimblock)

            self.recorder.debug( 'freeblocks', self.freeblocks)
            self.recorder.debug( 'usedblocks', self.usedblocks)

            cnt += 1
            if cnt % 10 == 0:
                # time to check
                if len(self.freeblocks) <= lastfree:
                    # Not making progress (number of free blocks are not increased)
                    self.recorder.debug( self.bitmap.bitmap )
                    self.recorder.debug('GC is not making progress! End GC')
                    break
                else:
                    lastfree = len(self.freeblocks)

        self.recorder.debug('==================================garbage collecting ends')

    def show_map(self):
        self.recorder.debug(self.l2p)
        self.recorder.debug(self.p2l)

    def debug(self):
        self.show_map()
        self.recorder.debug( 'VALIDBITMAP', self.bitmap.bitmap)
        self.recorder.debug( 'FREEBLOCKS ', self.freeblocks)
        self.recorder.debug( 'USEDBLOCKS ', self.usedblocks)

