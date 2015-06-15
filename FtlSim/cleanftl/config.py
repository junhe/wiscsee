import json
import os

class Config(dict):
    def show(self):
        print self

    def load_from_dict(self, dic):
        super(Config, self).clear()
        super(Config, self).__init__(dic)

    def load_from_json_file(self, file_path):
        decoded = json.load(open(file_path, 'r'))
        self.load_from_dict(decoded)

    def byte_to_pagenum(self, offset):
        "offset to page number"
        assert offset % self['flash_page_size'] == 0, \
                'offset: {off}, page_size: {ps}'.format(off=offset, ps = flash_page_size)
        return offset / self['flash_page_size']

    def page_to_block(self, pagenum):
        d = {}
        d['blocknum'] = pagenum / self['flash_npage_per_block']
        d['pageoffset'] = pagenum % self['flash_npage_per_block']
        return d

    def page_to_block_off(self, pagenum):
        "return block, page_offset"
        return pagenum / self['flash_npage_per_block'], \
                pagenum % self['flash_npage_per_block']

    def block_off_to_page(self, blocknum, pageoff):
        "convert block number and page offset to page number"
        return blocknum * self['flash_npage_per_block'] + pageoff

    def block_to_page_range(self, blocknum):
        return blocknum * self['flash_npage_per_block'], \
                (blocknum + 1) * self['flash_npage_per_block']

    def total_num_pages(self):
        return self['flash_npage_per_block'] * self['flash_num_blocks']

    def off_size_to_page_list(self, off, size):
        assert size % self['flash_page_size'] == 0
        npages = size / self['flash_page_size']
        start_page = self.byte_to_pagenum(off)

        return range(start_page, start_page+npages)

    def get_output_file_path(self):
        return os.path.join(self['output_dir'], 'ftlsim.out')

# need explicit initialization for it to be usable
conf = Config()

# a = Config()
# a['2'] = 3
# a.show()
# a.load_from_dict({4:88})
# a.show()
# a.load_from_json_file('./config.json')
# a.show()

