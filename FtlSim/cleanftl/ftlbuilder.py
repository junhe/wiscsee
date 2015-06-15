
class FtlBuilder(object):
    def __init__(self, confobj, recorder, flash):
        self.conf = confobj
        self.recorder = recorder
        self.flash = flash

    def lba_read(self, page_num):
        raise NotImplementedError

    def lba_write(self, page_num):
        raise NotImplementedError

    def lba_discard(self, page_num):
        raise NotImplementedError

    def debug_info(self):
        raise NotImplementedError

