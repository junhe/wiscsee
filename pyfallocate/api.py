import _fallocate

def fallocate(fd, mode, offset, length):
    ret = _fallocate.lib.fallocate(fd, mode, offset, length)
    return ret


