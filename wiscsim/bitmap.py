import bitarray
import config

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
        self.bitmap.setall(0)

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

        return cnt / float(self.conf.n_pages_per_block)

    def block_valid_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        cnt = 0
        for pg in range(start, end):
            if self.is_page_valid(pg):
                cnt += 1

        ret = cnt / float(self.conf.n_pages_per_block)
        return ret

    def block_erased_ratio(self, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        cnt = 0
        for pg in range(start, end):
            if self.is_page_erased(pg):
                cnt += 1

        ret = cnt / float(self.conf.n_pages_per_block)
        return ret

    def is_page_valid(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        return self.bitmap[s:e] == self.VALID

    def is_page_invalid(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        return self.bitmap[s:e] == self.INVALID

    def is_page_erased(self, pagenum):
        s, e = self.pagenum_to_slice_range(pagenum)
        return self.bitmap[s:e] == self.ERASED

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
            raise RuntimeError("page {} state is not recognized: {}".format(
                pagenum, state))

    def initialize(self):
        """ this method should be called in FTL """
        # set the state of all pages to ERASED
        self.bitmap.setall(0)


