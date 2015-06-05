import bmftl
import config
import dmftl
import hmftl
import pmftl
import recorder

if config.ftl_type == 'directmap':
    ftlinst = dmftl
elif config.ftl_type == 'pagemap':
    ftlinst = pmftl
elif config.ftl_type == 'blockmap':
    ftlinst = bmftl
elif config.ftl_type == 'hybridmap':
    ftlinst = hmftl
else:
    ftlinst = None

def lba_read(pagenum):
    return ftlinst.lba_read(pagenum)

def lba_write(pagenum):
    return ftlinst.lba_write(pagenum)

def lba_discard(pagenum):
    return ftlinst.lba_discard(pagenum)

def debug_after_processing():
        ftlinst.ftl.debug()

