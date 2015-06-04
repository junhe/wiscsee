import bmftl
import config
import dmftl
import pmftl
import recorder

def lba_read(pagenum):
    if config.ftl_type == 'directmap':
        return dmftl.lba_read(pagenum)
    elif config.ftl_type == 'pagemap':
        return pmftl.lba_read(pagenum)
    elif config.ftl_type == 'blockmap':
        return bmftl.lba_read(pagenum)

def lba_write(pagenum):
    if config.ftl_type == 'directmap':
        return dmftl.lba_write(pagenum)
    elif config.ftl_type == 'pagemap':
        return pmftl.lba_write(pagenum)
    elif config.ftl_type == 'blockmap':
        return bmftl.lba_write(pagenum)

def lba_discard(pagenum):
    if config.ftl_type == 'directmap':
        return dmftl.lba_discard(pagenum)
    elif config.ftl_type == 'pagemap':
        return pmftl.lba_discard(pagenum)
    elif config.ftl_type == 'blockmap':
        return bmftl.lba_discard(pagenum)

def debug_after_processing():
    pmftl.ftl.show_map()
    recorder.debug( 'VALIDBITMAP', pmftl.ftl.validbitmap)
    recorder.debug( 'FREEBLOCKS ', pmftl.ftl.freeblocks)
    recorder.debug( 'USEDBLOCKS ', pmftl.ftl.usedblocks)

