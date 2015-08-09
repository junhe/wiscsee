import json
import math
import os

WLRUNNER, LBAGENERATOR = ('WLRUNNER', 'LBAGENERATOR')

class Config(dict):
    def show(self):
        print self

    def load_from_dict(self, dic):
        super(Config, self).clear()
        super(Config, self).__init__(dic)

    def load_from_json_file(self, file_path):
        decoded = json.load(open(file_path, 'r'))
        self.load_from_dict(decoded)

    def dump_to_file(self, file_path):
        with open(file_path, "w") as f:
            json.dump(self, f, indent=4)

    def get_blkparse_result_path(self):
        return os.path.join(self['result_dir'], 'blkparse-output.txt')

    def get_blkparse_result_path_mkfs(self):
        "for file system making"
        return os.path.join(self['result_dir'], 'blkparse-output-mkfs.txt')

    def get_blkparse_result_table_path(self):
        return os.path.join(self['result_dir'], 'blkparse-output-table.txt')

    def get_ftlsim_events_output_path(self):
        "This is the path to output parsed blkparse results"
        return os.path.join(self['result_dir'],
            'blkparse-events-for-ftlsim.txt')

    def get_ftlsim_events_output_path_mkfs(self):
        "This is the path to output parsed blkparse results"
        return os.path.join(self['result_dir'],
            'blkparse-events-for-ftlsim-mkfs.txt')

    def byte_to_pagenum(self, offset, force_alignment = True):
        "offset to page number"
        if force_alignment and offset % self['flash_page_size'] != 0:
            raise RuntimeError('offset: {off}, page_size: {ps}'.format(
                off=offset, ps = self['flash_page_size']))
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

    def off_size_to_page_list(self, off, size, force_alignment = True):
        if force_alignment:
            assert size % self['flash_page_size'] == 0, \
                'size:{}, flash_page_size:{}'.format(size, self['flash_page_size'])
            npages = size / self['flash_page_size']
            start_page = self.byte_to_pagenum(off)

            return range(start_page, start_page+npages)
        else:
            start_page = self.byte_to_pagenum(off, force_alignment = False)
            npages = int(math.ceil(float(size) / self['flash_page_size']))

            return range(start_page, start_page+npages)

    def off_size_to_page_range(self, off, size, force_alignment = True):
        "The input is in bytes"
        if force_alignment:
            assert size % self['flash_page_size'] == 0, \
                'size:{}, flash_page_size:{}'.format(size, self['flash_page_size'])
            npages = size / self['flash_page_size']
            start_page = self.byte_to_pagenum(off)

            return start_page, npages
        else:
            start_page = self.byte_to_pagenum(off, force_alignment = False)
            npages = int(math.ceil(float(size) / self['flash_page_size']))

            return start_page, npages

    def get_output_file_path(self):
        return os.path.join(self['result_dir'], 'ftlsim.out')

    def set_flash_num_blocks_by_bytes(self, size_byte):
        self['flash_num_blocks'] = size_byte/ \
            (self['flash_page_size'] * self['flash_npage_per_block'])

    def dftl_n_mapping_entries_per_page(self):
        "used by dftl, return the number of mapping entries in a page"
        return self['flash_page_size'] \
            / self['dftl']['global_mapping_entry_bytes']

    def dftl_lpn_to_m_vpn(self, lpn):
        n_entries_per_page = self.dftl_n_mapping_entries_per_page()
        return lpn / n_entries_per_page

