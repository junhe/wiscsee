import os
from os import O_RDWR, O_CREAT
from accpatterns.patterns import READ, WRITE, DISCARD
from pyfallocate import fallocate


class File(object):
    def __init__(self, filepath):
        self.filepath = filepath
        self.fd = None

    def open(self):
        self.fd = os.open(self.filepath, O_RDWR|O_CREAT, 0666)

    def close(self):
        os.close(self.fd)
        self.fd = None

    def access(self, req_iter):
        for req in req_iter:
            if req.op == WRITE:
                os.lseek(self.fd, req.offset, os.SEEK_SET)
                os.write(self.fd, 'x' * req.size)
            elif req.op == READ:
                os.lseek(self.fd, req.offset, os.SEEK_SET)
                os.read(self.fd, req.size)
            elif req.op == DISCARD:
                fallocate(self.fd, 3, req.offset, req.size)


