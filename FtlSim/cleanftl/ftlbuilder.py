import bitarray

import config
import flash
import recorder

class FlashBitmap(object):
    VALID, INVALID = (True, False)

    def __init__(self, conf):
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not conf.Config")

        self.conf  = conf

        self.bitmap = bitarray.bitarray(conf.total_num_pages())

    def validate_page(self, pagenum):
        self.bitmap[pagenum] = FlashBitmap.VALID

    def invalidate_page(self, pagenum):
        self.bitmap[pagenum] = FlashBitmap.INVALID

    def validate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self.bitmap[start : end] = FlashBitmap.VALID

    def invalidate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self.bitmap[start : end] = FlashBitmap.INVALID

    def block_invalid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return self.bitmap[start:end].count(FlashBitmap.INVALID) / \
            float(self.conf['flash_npage_per_block'])

    def is_page_valid(self, pagenum):
        return self.bitmap[pagenum]

    def initialize(self):
        self.bitmap.setall(FlashBitmap.INVALID)

class FtlBuilder(object):
    def __init__(self, confobj, recorderobj, flashobj):
        if not isinstance(confobj, config.Config):
            raise TypeError('confobj is not of type config.Config')
        if not isinstance(recorderobj, recorder.Recorder):
            raise TypeError('recorder is not of type recorder.Recorder')
        if not isinstance(flashobj, flash.Flash):
            raise TypeError('flash is not of type flash.Flash')

        self.conf = confobj
        self.recorder = recorderobj
        self.flash = flashobj

        self.bitmap = FlashBitmap(self.conf)

    def lba_read(self, page_num):
        raise NotImplementedError

    def lba_write(self, page_num):
        raise NotImplementedError

    def lba_discard(self, page_num):
        raise NotImplementedError

    def debug_info(self):
        raise NotImplementedError

