import bitarray

import config
import flash
import recorder

class FlashBitmap(bitarray.bitarray):
    VALID, INVALID = (True, False)

    def __init__(self, conf):
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not conf.Config")

        # self.conf  = conf

        print self
        print 'length before', self.length()
        print 'length of conf', len(conf)
        print super(FlashBitmap, self).__dict__
        super(FlashBitmap, self).__init__([22])
        # self(100)
        print 'length after', self.length()

        raise RuntimeError('xxx')

    def validate_page(self, pagenum):
        self[pagenum] = FlashBitmap.VALID

    def invalidate_page(self, pagenum):
        self[pagenum] = FlashBitmap.INVALID

    def validate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self[start : end] = FlashBitmap.VALID

    def invalidate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self[start : end] = FlashBitmap.INVALID

    def block_invalid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return self[start:end].count(FlashBitmap.INVALID) / \
            float(self.conf['flash_npage_per_block'])

    def is_page_valid(self, pagenum):
        return self[pagenum]

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

