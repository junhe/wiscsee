from config import *

def byte_to_pagenum(offset):
    "offset to page number"
    assert offset % flash_page_size == 0, \
            'offset: {off}, page_size: {ps}'.format(off=offset, ps = flash_page_size)
    return offset / flash_page_size

def page_to_block(pagenum):
    d = {}
    d['blocknum'] = pagenum / flash_npage_per_block
    d['pageoffset'] = pagenum % flash_npage_per_block
    return d

def page_to_block_off(pagenum):
    "return block, page_offset"
    return pagenum / flash_npage_per_block, \
            pagenum % flash_npage_per_block

def block_off_to_page(blocknum, pageoff):
    "convert block number and page offset to page number"
    return blocknum * flash_npage_per_block + pageoff

def block_to_page_range(blocknum):
    return blocknum * flash_npage_per_block, \
            (blocknum + 1) * flash_npage_per_block

def total_num_pages():
    return flash_npage_per_block * flash_num_blocks

def off_size_to_page_list(off, size):
    assert size % flash_page_size == 0
    npages = size / flash_page_size
    start_page = byte_to_pagenum(off)

    return range(start_page, start_page+npages)

def load_json(fpath):
    decoded = json.load(open(fpath, 'r'))
    return decoded


