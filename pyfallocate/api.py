import _fallocate

#define FALLOC_FL_KEEP_SIZE     0x01 /* default is extend size */
#define FALLOC_FL_PUNCH_HOLE    0x02 /* de-allocates range */
FALLOC_FL_KEEP_SIZE = 1
FALLOC_FL_PUNCH_HOLE = 2

def fallocate(fd, mode, offset, length):
    ret = _fallocate.lib.fallocate(fd, mode, offset, length)
    return ret


