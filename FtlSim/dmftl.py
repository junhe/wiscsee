import flash
import common

def lba_read(offset, size):
    pagenum = common.byte_to_pagenum(offset)
    flash.page_read(pagenum)

def lba_write(offset, size):
    pass

def lba_discard(offset, size):
    pass

