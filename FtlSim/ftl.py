import dmftl

def lba_read(offset, size):
    return dmftl.lba_read(offset, size)

def lba_write(offset, size):
    return dmftl.lba_write(offset, size)

def lba_discard(offset, size):
    return dmftl.lba_discard(offset, size)

