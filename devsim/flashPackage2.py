import simpy

class Package:
    """
    This implementation is flat, to make it faster.
    """
    def __init__(self, env, conf):
        self.env = env
        self.conf = conf

        self.page_read_time = self.conf['page_read_time']
        self.page_prog_time = self.conf['page_prog_time']
        self.block_erase_time = self.conf['block_erase_time']

        self.n_pages_per_block = self.conf['n_pages_per_block']
        self.n_blocks_per_plane = self.conf['n_blocks_per_plane']
        self.n_planes_per_chip = self.conf['n_planes_per_chip']
        self.n_chips_per_package = self.conf['n_chips_per_package']
        self.n_pages_per_plane = self.conf['n_pages_per_plane']
        self.n_pages_per_chip = self.conf['n_pages_per_chip']
        self.n_pages_per_package = self.conf['n_pages_per_package']

        self.page_values = [None for i in range(self.n_pages_per_package)]

    def pagenum_of(self, chip_off, plane_off, block_off, page_off):
        return chip_off * n_pages_per_chip + \
                plane_off * n_pages_per_plane + \
                block_off * n_pages_per_block + \
                page_off

    def assert_page_range(self, page_off):
        assert page_off < self.n_pages_per_package, \
            "page_off: {}, n_pages_per_package: {}".format(
            page_off, self.n_pages_per_package)

    def read_page(self, page_off):
        self.assert_page_range(page_off)
        yield self.env.timeout( self.page_read_time )
        return self.page_values[page_off]

    def write_page(self, page_off, value):
        self.assert_page_range(page_off)
        yield self.env.timeout( self.page_prog_time )
        self.page_values[page_off] = value

    def erase_block(self, block_off):
        assert block_off * self.n_pages_per_block < self.n_pages_per_package
        yield self.env.timeout( self.block_erase_time )


