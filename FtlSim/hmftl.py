import bitarray
from collections import deque
import os

import bidict

import config
import ftlbuilder
import recorder


# Design notes
# What happen when a lba_block -> flash_block mapping exists and we
# write to a page in lba_block? For example, lba_block 518->flash 882
# already exists, and we now write to the first lba page, k, in lba block 518.
# In the current design, we will
# 1. find a free flash page k' for writing page k
# 2. write to page k'
# 3. set the end of log to k'
# 4. invalidate lba page k
#    if k is in page mapping:
#        find k's old flash page k'' and invalidate the flash page k''
#        remove k from log page mapping
#    if k's block is in block mappping
#        find k's old flash page k'' and invalidate the flash page k''
#        block map stays the same
# 5. add new log page mapping k -> k'
# 6. validate k'
#
# this could lead to the case that a lba_block -> flash_block exists
# but none of the pages in flash_block is valid (all of them have been
# overwritten and are in log blocks). When we do switch merge (or other
# merges?), we will want to switch the block in log blocks to data blocks
# but we will found the lba_block->flash_block already exists.
# The solution is to
# 1. consider adding mapping k->k' when k->k'' exist legal
# 2. remove block mapping when all pages in a block are invalid
# 1?
#

# TODO: improving cooperation between bitmap and mappings

class HybridMapping():
    """
    This mapping class uses bidict to manage the mapping between logical
    address and physical address. The advantage is that it is naturally
    symmetric, which means logical to physical and physical to logical is
    always one to one. Hmm.. example: if logical 100 maps to physical 200,
    then physical 200 has to map back to 100. It cannot map to other value.
    """
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not conf.Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj

        # for data block
        self.data_blk_l2p = bidict.bidict()
        # for log page
        self.log_page_l2p = bidict.bidict()

    def has_lba_block(self, lba_block):
        return lba_block in self.data_blk_l2p

    def has_flash_block(self, flash_block):
        return flash_block in self.data_blk_l2p.inv

    def has_lba_page(self, lba_page):
        return lba_page in self.log_page_l2p

    def has_flash_page(self, flash_page):
        return flash_page in  self.log_page_l2p.inv

    def lba_block_to_flash_block(self, lba_block):
        return self.data_blk_l2p[lba_block]

    def flash_block_to_lba_block(self, flash_block):
        return self.data_blk_l2p[:flash_block]

    def lba_page_to_flash_page(self, lba_page):
        return self.log_page_l2p[lba_page]

    def lba_page_to_flash_page_by_all_means(self, lba_pagenum):
        lba_block, lba_off = self.conf.page_to_block_off(lba_pagenum)

        if self.has_lba_page(lba_pagenum):
            return self.lba_page_to_flash_page(lba_pagenum)
        elif self.has_lba_block(lba_block):
            flash_block = self.lba_block_to_flash_block(lba_block)
            return self.conf.block_off_to_page(flash_block, lba_off)
        else:
            return None

    def flash_page_to_lba_page_by_all_means(self, flash_page):
        flash_block, flash_off = self.conf.page_to_block_off(flash_page)

        if self.has_flash_page(flash_page):
            return self.flash_page_to_lba_page(flash_page)
        elif self.has_flash_block[flash_block]:
            lba_block = self.flash_block_to_lba_block(flash_block)
            return self.conf.block_off_to_page(lba_block, flash_off)
        else:
            return None

    def flash_page_to_lba_page(self, flash_page):
        return self.log_page_l2p[:flash_page]

    def add_data_blk_mapping(self, lba_block, flash_block):
        # if lba_block in self.data_blk_l2p:
            # raise RuntimeError("Trying to add data block mapping "\
                # "{lba_block}->{flash_block}. But logical block {lba_block}"\
                # "->{old_flash_block} already exists".format(
                # lba_block = lba_block, flash_block = flash_block,
                # old_flash_block = self.lba_block_to_flash_block(lba_block)))
        if flash_block in self.data_blk_l2p.inv:
            raise RuntimeError("Trying to add data block mapping "\
                "{lba_block}->{flash_block}. But "\
                "{old_lba_block}->{flash_block} "\
                "already exists".format(
                old_lba_block = self.flash_block_to_lba_block(flash_block),
                lba_block = lba_block,
                flash_block = flash_block))
        self.data_blk_l2p[lba_block] = flash_block

    def remove_data_blk_mapping(self, lba_block=None, flash_block=None):
        "The mapping must exist. bidict should raise an exception."
        arg_bool = [lba_block == None , flash_block == None]
        if not any(arg_bool) or all(arg_bool) :
            # XOR
            raise ValueError("You should have specified one and only one "\
                "of lba_block and flash_block.")

        if lba_block != None:
            del self.data_blk_l2p[lba_block]
        if flash_block != None:
            del self.data_blk_l2p[:flash_block]

    def add_log_page_mapping(self, lba_page, flash_page):
        if lba_page in  self.log_page_l2p:
            raise RuntimeError("Trying to add data page mapping "\
                "{lba_page}->{flash_page}. But logical page {lba_page} "\
                "already exists".format(lba_page = lba_page,
                flash_page = flash_page))
        if flash_page in self.log_page_l2p.inv:
            raise RuntimeError("Trying to add data page mapping "\
                "{lba_page}->{flash_page}. But flash page {flash_page} "\
                "already exists".format(lba_page = lba_page,
                flash_page = flash_page))
        self.log_page_l2p[lba_page] = flash_page

    def remove_log_page_mapping(self, lba_page=None, flash_page=None):
        "The mapping must exist. bidict should raise an exception."
        arg_bool = [lba_page == None , flash_page == None]
        if not any(arg_bool) or all(arg_bool) :
            # XOR
            raise ValueError("You should have specified one and only one "\
                "of lba_page and flash_page.")
        if lba_page != None:
            del self.log_page_l2p[lba_page]
        if flash_page != None:
            del self.log_page_l2p[:flash_page]

class HybridBlockPool(object):
    def __init__(self, num_blocks):
        self.freeblocks = deque(range(num_blocks))

        # initialize usedblocks
        self.log_usedblocks = []
        self.data_usedblocks = []

    def pop_a_free_block(self):
        if self.freeblocks:
            blocknum = self.freeblocks.popleft()
        else:
            # nobody has free block
            raise RuntimeError('No free blocks in device!!!!')

        return blocknum

    def pop_a_free_block_to_log(self):
        "take one block from freelist and add it to log block list"
        blocknum = self.pop_a_free_block()
        self.log_usedblocks.append(blocknum)
        return blocknum

    def pop_a_free_block_to_data(self):
        "take one block from freelist and add it to data block list"
        blocknum = self.pop_a_free_block()
        self.data_usedblocks.append(blocknum)
        return blocknum

    def move_used_data_block_to_free(self, blocknum):
        self.data_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def move_used_log_block_to_free(self, blocknum):
        self.log_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def move_used_log_block_to_data_block(self, blocknum):
        self.log_usedblocks.remove(blocknum)
        self.data_usedblocks.append(blocknum)

    def __repr__(self):
        ret = ' '.join('freeblocks', repr(self.freeblocks)) + '\n' + \
            ' '.join('log_usedblocks', repr(self.log_usedblocks)) + '\n' \
            ' '.join('data_usedblocks', repr(self.data_usedblocks)) + '\n'

class GcDecider(object):
    """
    It decides whether we should do garbage collection. The decision
    is based on the number of used log blocks.

    This should be used as a local variable so the instance will be
    destroyed after each use. So we don't need to worry about global
    states.
    """
    def __init__(self, ftlobj):
        if not isinstance(ftlobj, HybridMapFtl):
            raise TypeError('ftlobj is not of Type HybridMapFtl')

        self.call_count = 0
        self.ftlobj = ftlobj

    def need_log_block_gc(self):
        """
        when called for the first time, compare used log blocks to
        high threshold.
        After the first time, compare used log blocks to low threshold
        """
        doit = None
        if self.call_count == 0:
            # first time
            if len(self.ftlobj.block_pool.log_usedblocks)\
                >= self.ftlobj.log_high_num_blocks:
                doit = True
            else:
                doit =  False
        else:
            # do GC until used blocks are less than low water mark
            if len(self.ftlobj.block_pool.log_usedblocks)\
                >= self.ftlobj.log_low_num_blocks:
                doit = True
            else:
                doit =  False

        self.call_count += 1
        return doit

    def debug_info(self):
        info = "log_usedblocks: {}, log_high_num_blocks: {} "\
            "log_low_num_blocks: {}".format(
            len(self.ftlobj.block_pool.log_usedblocks),
            self.ftlobj.log_high_num_blocks,
            self.ftlobj.log_low_num_blocks)
        return info



class HybridMapFtl(ftlbuilder.FtlBuilder):
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
    def __init__(self, confobj, recorderobj, flashobj):
        super(HybridMapFtl, self).__init__(confobj, recorderobj, flashobj)

        self.bitmap.initialize()

        self.mappings = HybridMapping(confobj)

        self.log_end_pagenum = -1 # the page number of the last write

        # initialize free list
        self.block_pool = HybridBlockPool(self.conf['flash_num_blocks'])

        self.log_high_num_blocks = int(self.conf['high_log_block_ratio']
            * self.conf['flash_num_blocks'])
        self.log_low_num_blocks = self.log_high_num_blocks * 0.8
        self.data_high_num_blocks = int(self.conf['high_data_block_ratio']
            * self.conf['flash_num_blocks'])

        # collect GC info
        # self.gcrec = recorder.Recorder(recorder.FILE_TARGET,
            # path=os.path.join(self.conf['result_dir'], 'gc.log'),
            # verbose_level = -1)

        self.gc_cnt_rec = recorder.Recorder(recorder.FILE_TARGET,
            path=os.path.join(self.conf['result_dir'], 'gc_cnt.log'),
            verbose_level = 3)

    def lba_read(self, pagenum):
        self.recorder.put('lba_read', pagenum, 'user')
        self.flash.page_read(pagenum, 'user')

    def lba_write(self, pagenum):
        self.recorder.put('lba_write', pagenum, 'user')
        self.write_page(pagenum, garbage_collect_enable=True, cat='user')

    def lba_discard(self, pagenum):
        self.recorder.put('lba_discard', pagenum, 'user')
        self.invalidate_lba_page(pagenum)

    def is_log_map_overflow(self):
        "it checks if there are too many log page mapping"
        if len(self.block_pool.log_usedblocks) > \
            self.conf['log_block_upperbound_ratio'] * self.conf['flash_num_blocks']:
            return True
        else:
            return False

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

    def invalidate_lba_page(self, lbapagenum):
        "invalidate bitmap and remove the mapping"
        lba_block, lba_off = self.conf.page_to_block_off(lbapagenum)

        if self.mappings.has_lba_page(lbapagenum):
            # in log block
            flashpagenum = self.mappings.lba_page_to_flash_page(lbapagenum)
            assert self.bitmap.is_page_valid(flashpagenum), 'WTF, in map but not valid?'
            self.bitmap.invalidate_page(flashpagenum)
            self.mappings.remove_log_page_mapping(lba_page=lbapagenum)
        elif self.mappings.has_lba_block(lba_block):
            # in data block
            # In this case, it is OK to be in map but not valid, because this
            # is block map
            flashpagenum = self.mappings.lba_page_to_flash_page_by_all_means(lbapagenum)
            self.bitmap.invalidate_page(flashpagenum)
        else:
            self.recorder.warning('trying to invalidate lba page {}, '\
                    'which is not in any map'.format(lbapagenum))

    def new_block_sanity_check(self, block):
        if self.mappings.has_flash_block(block):
            lba_block = self.mappings.flash_block_to_lba_block(block)
            raise RuntimeError("Flash block {flashblock} is new "\
                "from free block list. But it has mapping "\
                "{lbablock}->{flashblock}".format(
                flashblock = block,lbablock = lba_block))

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
            # get a new block
            block = self.block_pool.pop_a_free_block_to_log()
            self.new_block_sanity_check(block)
            start, _ = self.conf.block_to_page_range(block)
            return start

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
        lba_block, lba_off = self.conf.page_to_block_off(lba_pagenum)

        toflashpage = self.next_page_to_program()
        self.recorder.debug('Writing LBA {} to {}'.format(lba_pagenum, toflashpage))
        assert self.bitmap.is_page_valid(toflashpage) == False
        self.flash.page_write(toflashpage, cat)
        self.log_end_pagenum = toflashpage

        # this also removes mapping from lba_pagenum
        # writing this lba_pagenum could be an overwrite
        self.invalidate_lba_page(lba_pagenum)

        self.mappings.add_log_page_mapping(lba_page=lba_pagenum, flash_page=toflashpage)
        self.bitmap.validate_page(toflashpage)

        # do garbage collection if necessary
        if garbage_collect_enable and self.need_garbage_collection():
            self.garbage_collect()


    ############################# Garbage Collection ##########################
    def need_garbage_collection(self):
        if len(self.block_pool.log_usedblocks) >= self.log_high_num_blocks or \
            len(self.block_pool.data_usedblocks) >= self.data_high_num_blocks:
            return True
        else:
            return False

    def create_gc_decider(self):
        return GcDecider(self)

    def garbage_collect(self):
        # it is worth doing the most cost effective work first -
        # simply reclaim all DATA blocks without valid pages
        self.garbage_collect_data_blocks()

        self.garbage_collect_log_blocks()
        self.garbage_collect_merge()
        self.garbage_collect_data_blocks()

    def next_victim_log_block_to_merge(self):
        """
        Return the block with the highest invalid ratio
        Return None if there is no block in log_usedblocks
        use stupid for the prototype
        """
        maxratio = -1
        maxblock = None
        # we don't want usedblocks[-1] because it is the one in use, newly popped block
        # is appended to the used block list
        for blocknum in self.block_pool.log_usedblocks[0:-1]:
            invratio = self.bitmap.block_invalid_ratio(blocknum)
            if invratio > maxratio:
                maxblock = blocknum
                maxratio = invratio

        if maxblock == None:
            self.recorder.debug("no block in log_usedblocks[]")

        return maxblock

    def next_victim_log_block(self):
        # Greedy algorithm
        maxratio = -1
        maxblock = None
        # we don't want usedblocks[-1] because it is the one in use, newly popped block
        # is appended to the used block list
        for blocknum in self.block_pool.log_usedblocks[0:-1]:
            invratio = self.bitmap.block_invalid_ratio(blocknum)
            if invratio > maxratio:
                maxblock = blocknum
                maxratio = invratio

        if maxratio == 0:
            self.recorder.debug("Cannot find victimblock maxratio is", maxratio)
            return None

        if maxblock == None:
            self.recorder.debug("no block in usedblocks[]")

        return maxblock

    def next_victim_data_block(self):
        "for block map, we can only garbage collect block with no valid pages at all"
        maxratio = -1
        maxblock = None

        for blocknum in self.block_pool.data_usedblocks:
            invratio = self.bitmap.block_invalid_ratio(blocknum)
            if invratio == 1:
                return blocknum

        return None

    def move_valid_pages(self, blocknum):
        # this function read the valid pages in blocknum block and
        # write it as new pages. This is part of the garbage collection
        # process
        # note that this function does not erase this block

        # note *end* is not in block blocknum
        start, end = self.conf.block_to_page_range(blocknum)

        # The loop below will invalidate all pages in this block
        for page in range(start, end):
            if self.bitmap.is_page_valid(page):
                # lba = self.p2l[page]
                self.flash.page_read(page, 'amplified')
                lba = self.mappings.flash_page_to_lba_page_by_all_means(page)
                self.write_page(lba, garbage_collect_enable=False, cat='amplified')

    def is_switch_mergable(self, flash_blocknum):
        """
        This is only for log block
        We define mergable to be that all pages in the block is valid and
        exactly corresponds to the lba block
        """
        flash_pg_start, flash_pg_end = self.conf.block_to_page_range(flash_blocknum)

        for flash_pg in range(flash_pg_start, flash_pg_end):
            if not self.bitmap.is_page_valid(flash_pg):
                return False
            lba_pg = self.mappings.flash_page_to_lba_page(flash_pg)
            if lba_pg % self.conf['flash_npage_per_block'] != \
                flash_pg % self.conf['flash_npage_per_block']:
                return False

        return True

    def switch_merge(self, flash_blocknum):
        """
        Before calling, you need to make sure that flash_block is switch mergable
        At beginning, the pages in flash_blocknum have mapping in log map
        you want to remove those and add block mapping in block map
        """
        self.gc_cnt_rec.put_and_count("GC_SWITCH_MERGE", "victimblock", flash_blocknum,
                'invaratio', self.bitmap.block_invalid_ratio(flash_blocknum))

        flash_pg_start, flash_pg_end = self.conf.block_to_page_range(flash_blocknum)
        lba_pg_start = self.mappings.flash_page_to_lba_page_by_all_means(
            flash_pg_start)
        lba_block, _ = self.conf.page_to_block_off(lba_pg_start)

        # removing log page mapping
        for pg in range(flash_pg_start, flash_pg_end):
            if not self.bitmap.is_page_valid(pg):
                raise RuntimeError("All page in switch merge should be valid."\
                    " But {flash_page} (in flash block {flash_block}) is not."\
                    .format(flash_page = pg, flash_block = flash_blocknum))
            self.mappings.remove_log_page_mapping(flash_page=pg)

        # add data block mapping
        self.mappings.add_data_blk_mapping(lba_block=lba_block,
            flash_block=flash_blocknum)

        # valid bitmap does not change

        # move the block from log blocks to data blocks
        self.block_pool.move_used_log_block_to_data_block(flash_blocknum)

        self.recorder.debug('SWITCH MERGE IS DONE')

    def aggregate_lba_block(self, lba_block, target_flash_block):
        """
        Given a lba block number, this function finds all its pages on flash,
        read them to memory, and write them to flash_block. flash_block has to
        be erased and writable.
        """
        self.recorder.debug2('In aggregate_lba_block from lba_block', lba_block,
                'to', target_flash_block)
        lba_start, lba_end = self.conf.block_to_page_range(lba_block)
        moved = False
        for lba_page in range(lba_start, lba_end):
            page_off = lba_page % self.conf['flash_npage_per_block']
            flash_page = self.mappings.lba_page_to_flash_page_by_all_means(
                lba_page)
            self.recorder.debug2('trying to move lba_page', lba_page,
                '(flash_page:', flash_page, ')')

            if flash_page != None:
                # mapping exists (this lba page is on device)
                moved = True

                self.flash.page_read(flash_page, 'amplified')

                # handle mapping
                # if the lba_page in in log page mapping, we need to
                # invalidate the its flash page and remove the mapping
                # from log_page_l2p
                # if it is in data block, we don't just need to invalidate
                # the flash page. We don't need to change the block mapping
                # since there may be other valid pages in the block.
                self.bitmap.invalidate_page(flash_page)
                if self.mappings.has_lba_page(lba_page):
                    # handle the page mapping case
                    # you only need to delete the page mapping because
                    # later we will establish block mapping
                    self.mappings.remove_log_page_mapping(lba_page=lba_page)

                target_page = self.conf.block_off_to_page(target_flash_block, page_off)
                self.flash.page_write(target_page, 'amplified')
                self.bitmap.validate_page(target_page)

                self.recorder.debug2('move lba', lba_page, '(flash:', flash_page,
                        ') to flash', target_page)

        # Now all pages of lba_block is in target_flash_block
        # we now need to handle the mappings
        if moved:
            if self.mappings.has_lba_block(lba_block=lba_block):
                # the lba block was in the mapping
                self.mappings.remove_data_blk_mapping(lba_block=lba_block)
                self.mappings.add_data_blk_mapping(lba_block=lba_block,
                    flash_block=target_flash_block)
            else:
                self.mappings.add_data_blk_mapping(lba_block=lba_block,
                    flash_block=target_flash_block)

        self.recorder.debug('End aggregate_lba_block:')

    def full_merge(self, flash_blocknum):
        """
        For each valid flash page, we find all other pages in the same lba
        block with it and move them to a new flash block.
        We need to add mapping for the new flash block
        """
        self.gc_cnt_rec.put_and_count("GC_FULL_MERGE", "victimblock", flash_blocknum,
                'invaratio', self.bitmap.block_invalid_ratio(flash_blocknum))

        flash_start, flash_end = self.conf.block_to_page_range(flash_blocknum)

        # find all the lba blocks of the pages in flash_blocknum
        lbablocks = set()
        for flash_pg in range(flash_start, flash_end):
            if self.bitmap.is_page_valid(flash_pg):
                lba_pg = self.mappings.flash_page_to_lba_page_by_all_means(
                    flash_pg)
                lba_blk, off = self.conf.page_to_block_off(lba_pg)
                lbablocks.add(lba_blk)

        # aggregate all the lba blocks
        for lba_block in lbablocks:
            new_data_block = self.block_pool.pop_a_free_block_to_data()
            self.new_block_sanity_check(new_data_block)
            self.aggregate_lba_block(lba_block, new_data_block)

        # Now block flash_blocknum should have no valid pages
        # we can now erase it and put it to free list
        # flash_blocknum is a log block, and all its page mapping
        # have been handled above, so we don't need to worry about
        # page mapping.
        self.erase_block(flash_blocknum, 'amplified')
        # self.log_usedblocks.remove(flash_blocknum)
        # self.freeblocks.append(flash_blocknum)
        self.block_pool.move_used_log_block_to_free(flash_blocknum)

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
        self.gc_cnt_rec.debug("entering garbage_collect_log_blocks()")
        lastused = len(self.block_pool.log_usedblocks)
        cnt = 0
        decider = self.create_gc_decider()
        # while self.need_garbage_collect_log():
        while decider.need_log_block_gc():
            # used too many log blocks, need to garbage collect some to
            # free some, hopefully
            victimblock = self.next_victim_log_block()
            if victimblock == None:
                # if next_victim_block() return None, it means
                # no block can be a victim
                self.gc_cnt_rec.debug("No victim block can be found "\
                    "at garbage_collect_log_blocks()")
                break
            self.gc_cnt_rec.put_and_count("GC_LOG", "victimblock", victimblock,
                    'invaratio', self.bitmap.block_invalid_ratio(victimblock))

            self.move_valid_pages(victimblock)
            #block erasure is always counted as amplified
            self.erase_block(victimblock, 'amplified')

            # move from used to free
            # self.log_usedblocks.remove(victimblock)
            # self.freeblocks.append(victimblock)
            self.block_pool.move_used_log_block_to_free(victimblock)

            cnt += 1
            if cnt % 10 == 0:
                # time to check
                if len(self.block_pool.log_usedblocks) >= lastused:
                    # Not making progress
                    self.recorder.debug( self.bitmap.bitmap )
                    self.recorder.debug('GC is not making progress! End GC')
                    break
                lastused = len(self.block_pool.log_usedblocks)

    def garbage_collect_merge(self):
        self.gc_cnt_rec.debug("entering garbage_collect_merge()")
        decider = self.create_gc_decider()
        while decider.need_log_block_gc():
            # Note that it is possible that block 1 is full merged
            # but block 2 is switch merged
            victimblock = self.next_victim_log_block_to_merge()
            if victimblock == None:
                self.gc_cnt_rec.debug("No USED block can be found "\
                    "at garbage_collect_merge()")
                break

            # The following function may trigger any type of merge
            # for this one block
            self.merge_log_block(victimblock)
        self.gc_cnt_rec.debug(decider.debug_info())
        self.gc_cnt_rec.debug("leaving garbage_collect_merge()")


    def garbage_collect_data_blocks(self):
        """
        When needed, we recall all blocks with no valid page
        """
        block_to_clean = self.next_victim_data_block()
        while block_to_clean != None:
            self.erase_block(block_to_clean, 'amplified')

            # now remove the mappings
            # some blocks may be used by has no mapping
            if self.mappings.has_flash_block(block_to_clean):
                self.mappings.remove_data_blk_mapping(flash_block=block_to_clean)

            # move it to free list
            # self.data_usedblocks.remove(block_to_clean)
            # self.freeblocks.append(block_to_clean)
            self.block_pool.move_used_data_block_to_free(block_to_clean)

            block_to_clean = self.next_victim_data_block()

        self.recorder.debug('=============garbage_collect_data_blocks======================garbage collecting ends')

    def debug(self):
        self.recorder.debug('log_page_l2p', self.mappings.log_page_l2p)

        self.recorder.debug('data_blk_l2p', self.mappings.data_blk_l2p)

        self.recorder.debug('* VALIDBITMAP', self.bitmap.bitmap)
        self.recorder.debug('* blocks', self.block_pool)

    def show_map(self):
        self.recorder.debug('log_page_l2p', self.mappings.log_page_l2p)
        self.recorder.debug('log_page_p2l', self.mappings.log_page_p2l)

        self.recorder.debug('data_blk_l2p', self.mappings.data_blk_l2p)
        self.recorder.debug('data_blk_p2l', self.mappings.data_blk_p2l)

    # Sanity checks
    def is_page_mapping_ok(self, flash_pg):
        if self.mappings.has_flash_page(flash_pg):
            lba_pg = self.mappings.flash_page_to_lba_page(flash_pg)
        else:
            return False

        if self.mappings.has_lba_page(lba_pg):
            flash_page2 = self.mappings.lba_page_to_flash_page(lba_pg)
            if flash_page2 != flash_pg:
                return False
        else:
            self.recorder.error('lba_pg', lba_pg, 'is not in log_page_l2p')
            return False

        return True

    def is_block_mapping_ok(self, flash_block):
        if self.mappings.has_flash_block(flash_block):
            lba_block = self.flash_block_to_lba_block(flash_block)
        else:
            return False

        if self.mappings.has_lba_block(lba_block=lba_block):
            flash_block2 = self.mappings.lba_block_to_flash_block(lba_block)
            if flash_block2 != flash_block:
                return False
        else:
            return False
        return True

    def is_block_total_ok(self):
        if len(self.block_pool.freeblocks) + len(self.block_pool.log_usedblocks) + \
            len(self.block_pool.data_usedblocks) == self.conf['flash_num_blocks']:
            return True
        return False


    def is_sanity_check_ok(self):
        # if a page in valid, it must have mapping,
        # either block or page mapping
        for pg in range(self.conf.total_num_pages()):
            flash_block, off = self.conf.page_to_block_off(pg)
            if self.bitmap.is_page_valid(pg) and \
                not self.is_page_mapping_ok(pg) and \
                not self.is_block_mapping_ok(flash_block):
                "page is valid but could not find mapping"
                return False

        # the sum of freeblock, data_usedblocks, log_usedblocks
        # should be flash_num_blocks
        if self.is_block_total_ok() == False:
            self.recorder.debug("self.is_block_total_ok() is False")
            return False

        # log block is not overflow
        # if self.is_log_map_overflow() == True:
            # self.recorder.debug("self.is_log_map_overflow() is True")
            # return False

        return True


