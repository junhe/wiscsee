import bitarray

import flash
import ftlbuilder

class DirectMapFtl(ftlbuilder.FtlBuilder):

    def __init__(self, confobj, recorderobj, flashobj):
        # From parent:
        # self.conf = confobj
        # self.recorder = recorder
        super(DirectMapFtl, self).__init__(confobj, recorderobj, flashobj)

        self.bitmap.initialize()

    # implement abstract functions
    def lba_read(self, pagenum):
        self.flash.page_read(pagenum, 'user')

    def lba_write(self, pagenum):
        self.write_page(pagenum)

    def lba_discard(pagenum):
        self.bitmap.invalidate_page(pagenum)

    def read_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        for pagenum in range(start, end):
            self.flash.page_read(pagenum, 'unimplemented')

    def program_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        for pagenum in range(start, end):
            self.flash.page_write(pagenum, 'unimplemented')

    def erase_block(self, blocknum):
        self.flash.block_erase(blocknum, 'amplified')

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
        blocknum = self.conf.page_to_block(pagenum)['blocknum']
        self.read_block(blocknum)
        self.modify_page_in_ram(pagenum)
        self.erase_block(blocknum)
        self.program_block(blocknum)

