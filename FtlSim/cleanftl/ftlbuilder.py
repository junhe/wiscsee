
class FtlBuilder(object):
    def __init__(self, confobj, recorder):
        self.conf = confobj
        self.recorder = recorder

    def lba_read(self, page_num):
        raise NotImplementedError

    def lba_write(self, page_num):
        raise NotImplementedError

    def lba_discard(self, page_num):
        raise NotImplementedError

    def debug_info(self):
        raise NotImplementedError

