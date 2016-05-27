import os
from os import O_RDWR, O_CREAT, O_DIRECT
from accpatterns.patterns import READ, WRITE, DISCARD
from commons import *
from pyfallocate import fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE

from utilities.utils import drop_caches


class File(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.fd = None

    def open(self):
        self.fd = os.open(self.filepath, O_RDWR | O_CREAT, 0666)

    def close(self):
        os.close(self.fd)
        self.fd = None

    def access(self, req_iter):
        "req is accpatterns.patterns.Request"
        for req in req_iter:
            op = req.get_operation()
            if op == WRITE:
                os.lseek(self.fd, req.offset, os.SEEK_SET)
                os.write(self.fd, 'x' * req.size)
            elif op == OP_FSYNC:
                os.fsync(self.fd)
            elif op == OP_FDATASYNC:
                os.fdatasync(self.fd)
            elif op == READ:
                os.lseek(self.fd, req.offset, os.SEEK_SET)
                os.read(self.fd, req.size)
            elif op == DISCARD:
                fallocate(self.fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
                        req.offset, req.size)
            elif op == OP_DROPCACHE:
                drop_caches()
            elif op == OP_FALLOCATE:
                self._fallocate(req)
            else:
                print 'WARNING', op, 'is not supported in File adapter.'

    def _fallocate(self, req):
        zone_size = req.arg1

        if req.arg2 == OP_ARG_KEEPSIZE:
            fallocate(self.fd, FALLOC_FL_KEEP_SIZE, 0, zone_size)
        elif req.arg2 == OP_ARG_NOTKEEPSIZE:
            fallocate(self.fd, 0, 0, zone_size)


