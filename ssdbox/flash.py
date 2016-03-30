import simpy


class SimpleFlash(object):
    def __init__(self, recorder, confobj = None):
        self.recorder = recorder
        self.conf = confobj

        self.data = {} # ppn -> contents stored in a flash page

    def page_read(self, pagenum, cat):
        self.recorder.put('physical_read', pagenum, cat)

        content = self.data.get(pagenum, None)
        return content

    def page_write(self, pagenum, cat, data = None):
        self.recorder.put('physical_write', pagenum, cat)

        if data != None:
            self.data[pagenum] = data

    def block_erase(self, blocknum, cat):
        # print 'block_erase', blocknum, cat
        self.recorder.put('phy_block_erase', blocknum, cat)

        ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
        for ppn in range(ppn_start, ppn_end):
            try:
                del self.data[ppn]
            except KeyError:
                # ignore key error
                pass

class Flash(object):
    def __init__(self, recorder, confobj = None, globalhelper = None):
        self.recorder = recorder

        # If you enable store data, you must provide confobj
        self.store_data = True # whether store data to self.data[]
        self.data = {} # ppn -> contents stored in a flash page
        self.conf = confobj
        self.globalhelper = globalhelper

    def page_read(self, pagenum, cat):
        self.recorder.put('physical_read', pagenum, cat)

        self.globalhelper.timeline.incr_flash_op('flash.read')

        if self.store_data == True:
            content = self.data.get(pagenum, None)
            return content

    def page_write(self, pagenum, cat, data = None):
        self.recorder.put('physical_write', pagenum, cat)
        self.globalhelper.timeline.incr_flash_op('flash.write')

        # we only put data to self.data when the caller specify data
        if self.store_data == True:
            if data != None:
                self.data[pagenum] = data

    def block_erase(self, blocknum, cat):
        # print 'block_erase', blocknum, cat
        self.recorder.put('phy_block_erase', blocknum, cat)
        self.globalhelper.timeline.incr_flash_op('flash.erasure')

        if self.store_data == True:
            ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
            for ppn in range(ppn_start, ppn_end):
                try:
                    del self.data[ppn]
                except KeyError:
                    # ignore key error
                    pass






class FlashDES(object):
    def __init__(self, recorder, confobj = None, globalhelper = None,
            simpy_env = None):
        self.recorder = recorder

        # If you enable store data, you must provide confobj
        self.store_data = True # whether store data to self.data[]
        self.data = {} # ppn -> contents stored in a flash page
        self.conf = confobj
        self.globalhelper = globalhelper
        self.env = simpy_env

        self.page_read_time = self.conf['flash_config']['page_read_time']
        self.page_prog_time = self.conf['flash_config']['page_prog_time']
        self.block_erase_time = self.conf['flash_config']['block_erase_time']

    def page_read(self, pagenum, cat):
        self.recorder.put('physical_read', pagenum, cat)

        yield self.env.timeout( self.page_read_time )

        self.globalhelper.timeline.incr_flash_op('flash.read')

        if self.store_data == True:
            content = self.data.get(pagenum, None)
            self.env.exit(content)

    def page_write(self, pagenum, cat, data = None):
        self.recorder.put('physical_write', pagenum, cat)

        yield self.env.timeout( self.page_prog_time )

        self.globalhelper.timeline.incr_flash_op('flash.write')

        # we only put data to self.data when the caller specify data
        if self.store_data == True:
            if data != None:
                self.data[pagenum] = data

    def block_erase(self, blocknum, cat):
        # print 'block_erase', blocknum, cat
        self.recorder.put('phy_block_erase', blocknum, cat)

        yield self.env.timeout( self.block_erase_time )

        self.globalhelper.timeline.incr_flash_op('flash.erasure')

        if self.store_data == True:
            ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
            for ppn in range(ppn_start, ppn_end):
                try:
                    del self.data[ppn]
                except KeyError:
                    # ignore key error
                    pass



