class Flash(object):
    def __init__(self, recorder):
        self.recorder = recorder

    def page_read(self, pagenum, cat):
        # print 'page_read', pagenum, cat
        self.recorder.put('page_read', pagenum, cat)

    def page_write(self, pagenum, cat):
        # print 'page_write', pagenum, cat
        self.recorder.put('page_write', pagenum, cat)

    def block_erase(self, blocknum, cat):
        # print 'block_erase', blocknum, cat
        self.recorder.put('block_erase', blocknum, cat)

