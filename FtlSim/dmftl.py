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
        self.recorder.put('lba_read', pagenum, 'user')
        self.flash.page_read(pagenum, 'user')

    def lba_write(self, pagenum):
        self.recorder.put('lba_write', pagenum, 'user')
        self.write_page(pagenum)

    def lba_discard(pagenum):
        self.recorder.put('lba_discard ', pagenum, 'user')
        self.bitmap.invalidate_page(pagenum)

    def erase_block(self, blocknum):
        self.flash.block_erase(blocknum, 'amplified')

    def modify_page_in_ram(self, pagenum):
        "this is a dummy function"
        pass

    def write_page(self, pagenum, cat='user'):
        """
        in directmap we need to:
        1. read the whole block
        2. modify page in RAM buffer
        3. erase the block
        4. write the block
        """
        blocknum = self.conf.page_to_block(pagenum)['blocknum']

        start, end = self.conf.block_to_page_range(blocknum)
        for pg in range(start, end):
            if pg == pagenum:
                loop_cat = 'user'
            else:
                loop_cat = 'amplified'
            self.flash.page_read(pg, loop_cat)

        self.modify_page_in_ram(pagenum)

        self.erase_block(blocknum)

        for pg in range(start, end):
            if pg == pagenum:
                loop_cat = 'user'
            else:
                loop_cat = 'amplified'
            self.flash.page_write(pg, loop_cat)

