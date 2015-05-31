import dmftl

def lba_read(pagenum):
    return dmftl.lba_read(pagenum)

def lba_write(pagenum):
    return dmftl.lba_write(pagenum)

def lba_discard(pagenum):
    return dmftl.lba_discard(pagenum)

