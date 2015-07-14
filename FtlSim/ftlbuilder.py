import bitarray

import config
import flash
import recorder

class FlashBitmap1(object):
    "Using one bit to represent state of a page"
    VALID, INVALID = (True, False)

    def __init__(self, conf):
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not conf.Config. it is {}".
               format(type(conf).__name__))

        self.conf  = conf

        self.bitmap = bitarray.bitarray(conf.total_num_pages())

    def validate_page(self, pagenum):
        self.bitmap[pagenum] = self.VALID

    def invalidate_page(self, pagenum):
        self.bitmap[pagenum] = self.INVALID

    def validate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self.bitmap[start : end] = self.VALID

    def invalidate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        self.bitmap[start : end] = self.INVALID

    def block_invalid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return self.bitmap[start:end].count(self.INVALID) / \
            float(self.conf['flash_npage_per_block'])

    def is_page_valid(self, pagenum):
        return self.bitmap[pagenum]

    def page_bits(self, pagenum):
        return self.bitmap[pagenum]

    def block_bits(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        return self.bitmap[start : end]

    def initialize(self):
        self.bitmap.setall(self.INVALID)

class FlashBitmap2(object):
    "Using two bit to represent state of a page"
    ERASED, VALID, INVALID = (bitarray.bitarray('00'),
        bitarray.bitarray('01'), bitarray.bitarray('10'))

    def __init__(self, conf):
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not conf.Config. it is {}".
               format(type(conf).__name__))

        self.conf  = conf

        # We use two bits to record state of a page so that
        # we will be able to record ERASED state
        self.bitmap = bitarray.bitarray(2 * conf.total_num_pages())

    def pagenum_to_slice_range(self, pagenum):
        "2 is the number of bits representing the state of a page"
        return 2 * pagenum, 2 * (pagenum + 1)

    def blocknum_to_slice_range(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        s, _ = self.pagenum_to_slice_range(start)
        # not that end is the first page after the block, so
        # the first bit of page end is the first bit after the block,
        # not the second
        e, _ = self.pagenum_to_slice_range(end)

        return s, e

    def validate_page(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        self.bitmap[s:e] = self.VALID

    def invalidate_page(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        self.bitmap[s:e] = self.INVALID

    def validate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        for pg in range(start, end):
            self.validate_page(pg)

    def invalidate_block(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        for pg in range(start, end):
            self.validate_page(pg)

    def erase_block(self, blocknum):
        s, e = self.blocknum_to_slice_range(blocknum)
        self.bitmap[s:e] = 0

    def block_invalid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        cnt = 0
        for pg in range(start, end):
            if not self.is_page_valid(pg):
                cnt += 1

        return cnt / float(self.conf['flash_npage_per_block'])

    def is_page_valid(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        return self.bitmap[s:e] == self.VALID

    def page_bits(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        return self.bitmap[s:e]

    def block_bits(self, blocknum):
        s, e = self.blocknum_to_slice_range(blocknum)
        return self.bitmap[s:e]

    def page_state(self, pagenum):
        """
        This is usually for usage:
            if bmap.page_state(333) == bmap.VALID:
                do something
        """
        s, e = self.pagenum_to_slice_range(pagenum)
        return self.bitmap[s:e]

    def page_state_human(self, pagenum):
        state = self.page_state(pagenum)
        if state == self.VALID:
            return "VALID"
        elif state == self.INVALID:
            return "INVALID"
        elif state == self.ERASED:
            return "ERASED"
        else:
            raise RuntimeError("state is not recognized: {}".format(state))

    def initialize(self):
        """ this method should be called in FTL """
        # set the state of all pages to ERASED
        self.bitmap.setall(0)

class FtlBuilder(object):
    def __init__(self, confobj, recorderobj, flashobj):
        if not isinstance(confobj, config.Config):
            raise TypeError('confobj is not of type config.Config, it is {}'.
               format(type(confobj).__name__))
        if not isinstance(recorderobj, recorder.Recorder):
            raise TypeError('recorder is not of type recorder.Recorder, "\
                "it is{}'.format(type(recorderobj).__name__))
        if not isinstance(flashobj, flash.Flash):
            raise TypeError('flash is not of type flash.Flash'.
               format(type(flashobj).__name__))

        self.conf = confobj
        self.recorder = recorderobj
        self.flash = flashobj

        self.bitmap = FlashBitmap2(self.conf)

    def lba_read(self, page_num):
        raise NotImplementedError

    def lba_write(self, page_num):
        raise NotImplementedError

    def lba_discard(self, page_num):
        raise NotImplementedError

    def debug_info(self):
        raise NotImplementedError

if __name__ == '__main__':
    b = bitarray.bitarray(10)
    print b
    print b[0:2]
    print b[0:2] == [False, False]
    print b[0:2] & bitarray.bitarray('00')
    print b[0:2] | bitarray.bitarray('10')
    print b[0:2] == bitarray.bitarray('00')
    print b[0:2] == bitarray.bitarray('10')
    print b[0:2] == bitarray.bitarray('10')

    print b[0:2]
    print 'assign....'
    b[0:2] = FlashBitmap.ERASED
    print b[0:2]
    b[0:2] = FlashBitmap.VALID
    print b[0:2]
    b[0:2] = FlashBitmap.INVALID
    print b[0:2]
    b[0:5] = 1
    print b



