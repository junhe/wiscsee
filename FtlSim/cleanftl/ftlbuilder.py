import bitarray

import config

class FlashValidBitmap(bitarray.bitarray):
    def __init__(self, conf):
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not conf.Config")

        self.conf  = conf

        super(FlashValidBitmap, self).__init__(self.conf.total_num_pages())

    def validate_page(self, pagenum):
        self[pagenum] = True

    def invalidate_page(self, pagenum):
        self[pagenum] = False

    def validate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self[start : end] = True

    def invalidate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self[start : end] = False

    def block_invalid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return self[start:end].count(False) / \
            float(self.conf['flash_npage_per_block'])

    def is_page_valid(self, pagenum):
        return self[pagenum]

class FtlBuilder(object):
    def __init__(self, confobj, recorder, flash):
        self.conf = confobj
        self.recorder = recorder
        self.flash = flash

        self.bitmap = FlashValidBitmap(self.conf.total_num_pages())

    def lba_read(self, page_num):
        raise NotImplementedError

    def lba_write(self, page_num):
        raise NotImplementedError

    def lba_discard(self, page_num):
        raise NotImplementedError

    def debug_info(self):
        raise NotImplementedError

