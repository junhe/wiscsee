import recorder

def page_read(pagenum):
    recorder.put('page_read', pagenum)

def page_write(pagenum):
    recorder.put('page_write', pagenum)

def block_erase(blocknum):
    recorder.put('block_erase', blocknum)

