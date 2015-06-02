import recorder

def page_read(pagenum, cat):
    recorder.put('page_read', pagenum, cat)

def page_write(pagenum, cat):
    recorder.put('page_write', pagenum, cat)

def block_erase(blocknum, cat):
    recorder.put('block_erase', blocknum, cat)

