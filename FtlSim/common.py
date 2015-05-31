import config

def byte_to_pagenum(offset):
    "offset to page number"
    assert offset % config.flash_page_size == 0
    return offset / config.flash_page_size

