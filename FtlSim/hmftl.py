import flash
import config
from common import *
import bitarray

class Ftl:
    """
    Write a page (not program a page):
        - append the page to existing log blocks
            - there should be a list of blocks maitained as log blocks
              they must have order. A block can only be either log block
              or data block. So you don't need to maitain a list for data
              block - any block that is not in log block list is a data block.
              the number of log blocks has a limit (so their mapping table won't
              be too large).
        - check if the number of free log blocks is less than a threshold, if so,
          start merging log blocks to get some
    """

    def __init__(self, page_size, npages_per_block, num_blocks, n_log_blocks):
        self.page_size = page_size
        self.npages_per_block = npages_per_block
        self.num_blocks = num_blocks

        # initialize bitmap 1: valid, 0: invalid
        npages = num_blocks * npages_per_block
        self.validbitmap = bitarray.bitarray(npages)
        # all pages are valid at the beginning
        # assuming the vendor has done the first erasure
        self.validbitmap.setall(True)

    # bitmap operations
    def validate_page(self, pagenum):
        "use this function to wrap the operation, "\
        "in case I change bitmap module later"
        self.validbitmap[pagenum] = True

    def invalidate_page(self, pagenum):
        self.validbitmap[pagenum] = False

    def validate_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = True

    def invalidate_block(self, blocknum):
        start, end = block_to_page_range(blocknum)
        self.validbitmap[start : end] = False

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

    def write_page(self, pagenum):
        """
        in directmap we need to:
        1. read the whole block
        2. modify page in RAM buffer
        3. erase the block
        4. write the block
        """
        blocknum = page_to_block(pagenum)['blocknum']
        self.read_block(blocknum)
        self.modify_page_in_ram(pagenum)
        self.erase_block(blocknum)
        self.program_block(blocknum)

ftl = Ftl(config.flash_page_size,
          config.flash_npage_per_block,
          config.flash_num_blocks)

def lba_read(pagenum):
    flash.page_read(pagenum)

def lba_write(pagenum):
    ftl.write_page(pagenum)

def lba_discard(pagenum):
    pass



