import json
import math
import os
import pprint
import re

from commons import *
from utilities import utils

WLRUNNER, LBAGENERATOR, LBAMULTIPROC = ('WLRUNNER', 'LBAGENERATOR',
    'LBAMULTIPROC')

class MountOption(dict):
    """
    This class abstract the option of file system mount command.
    The initial motivation is to handle the difference, for example, between
    data=ordered and delalloc. Note that one of them has format opt_name=value,
    the other is just value.

    It inherite dict class so json module can serialize it.
    """
    def __init__(self, opt_name, value, include_name):
        """
        opt_name: such as discard
        value: such as discard, nodiscard
        include_name: such as True, False
        """
        self['opt_name'] = opt_name
        self['value'] = value
        self['include_name'] = include_name

    def __str__(self):
        if self['include_name']:
            prefix = self['opt_name'] + '='
        else:
            prefix = ''

        return prefix + str(self['value'])


class Config(dict):
    def __init__(self, confdic = None):
        if confdic == None:
            self.update(self.get_default_config())
        else:
            self.update(confdic)

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

    def get_blkparse_result_path_aging(self):
        return os.path.join(self['result_dir'], 'blkparse-output-aging.txt')

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

    def total_flash_bytes(self):
        return self['flash_npage_per_block'] * self['flash_num_blocks'] \
            * self['flash_page_size']

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
        nblocks = size_byte / \
            (self['flash_page_size'] * self['flash_npage_per_block'])

        rem = size_byte % \
            (self['flash_page_size'] * self['flash_npage_per_block'])

        print 'WARNING: set_flash_num_blocks_by_bytes() cannot set to '\
            'exact bytes. rem:', rem
        self['flash_num_blocks'] = nblocks


    def sec_ext_to_page_ext(self, sector, count):
        """
        The sector extent has to be aligned with page
        return page_start, page_count
        """
        page = (sector * self['sector_size']) / self['flash_page_size']
        assert (sector * self['sector_size']) % self['flash_page_size'] == 0,\
                "starting sector ({}) is not aligned with page size {}"\
                .format(sector, self['flash_page_size'])
        page_count = (count * self['sector_size']) / self['flash_page_size']
        assert (count * self['sector_size']) % self['flash_page_size'] == 0, \
                "total size {} bytes is not multiple of page size {} bytes."\
                .format(count * self['sector_size'], self['flash_page_size'])
        return page, page_count

    def off_size_to_sec_count(self, offset, size):
        """
        offset and size in byte -> sector and count
        """
        sector_size = self['sector_size']

        assert offset % sector_size == 0
        sector = offset / sector_size

        assert size % sector_size == 0
        count = size / sector_size

        return sector, count

    def get_default_config(self):
        MOpt = MountOption

        confdic = {
            ############### Global #########
            "result_dir"            : None,
            "workload_src"          : WLRUNNER,
            # "workload_src"          : LBAGENERATOR,
            "expname"               : "default-expname",
            "time"                  : None,
            "subexpname"            : "default-subexp",
            # directmap, blockmap, pagemap, hybridmap, dftl2, tpftl, nkftl
            "ftl_type"              : "nkftl2",
            "sector_size"           : 512,
            "sort_block_trace"      : True,
            "trace_issue_and_complete": False,

            ############## For wiscsim ######
            "enable_simulation"     : True,
            "flash_page_size"       : 4096,
            "flash_npage_per_block" : 4,
            "flash_num_blocks"      : 64,
            # "enable_e2e_test"       : False,
            "simulation_processor"  : 'e2e', # regular, extent
            "stripe_size"           : 4,  # unit: page
            "max_victim_valid_ratio": 0.9,
            "n_gc_procs"            : 1,

            "do_gc_after_workload"  : True,

            'snapshot_valid_ratios' : False,
            'snapshot_erasure_count_dist': False,
            'snapshot_interval': None,

            'wear_leveling_check_interval': 20*SEC,
            'do_wear_leveling'      : False,
            'wear_leveling_factor'  : 2,
            'wear_leveling_diff'    : 10,

            ############## hybridmap ############
            "high_log_block_ratio"       : 0.4,
            "high_data_block_ratio"      : 0.4,
            "hybridmapftl": {
                "low_log_block_ratio": 0.32
            },

            ############## recorder #############
            "verbose_level" : -1,
            "output_target" : "file",
            "print_when_finished": False,
            # "output_target" : "stdout",
            "record_bad_victim_block": False,

            ############## For workrunner ########
            "linux_ncq_depth"  : 128,
            "linux_io_scheduler" : 'noop',
            "dev_size_mb"      : None,
            "tmpfs_mount_point"     : "/mnt/tmpfs",

            "device_path"           : "/dev/loop0", # or sth. like /dev/sdc1
            "dev_padding"      : 8*MB,

            "enable_blktrace"       : False,
            "dump_ext4_after_workload"  : True,

            "event_file_column_names"    : ['pid', 'action', 'operation', 'offset', 'size',
                    'timestamp', 'pre_wait_time', 'sync'],

            "fs_mount_point"        : "/mnt/fsonloop",
            "mnt_opts" : {
                "ext4":   { 'discard': MOpt(opt_name = "discard",
                                             value = "discard",
                                             include_name = False),
                            'data': MOpt(opt_name = "data",
                                            value = "ordered",
                                            include_name = True) },
                "btrfs":  { "discard": MOpt(opt_name = "discard",
                                             value = "discard",
                                             include_name = False),
                                             "ssd": MOpt(opt_name = 'ssd',
                                                 value = 'ssd',
                                         include_name = False),
                            "autodefrag": MOpt(opt_name = 'autodefrag',
                                                value = 'autodefrag',
                                                include_name = False) },
                "xfs":    {'discard': MOpt(opt_name = 'discard',
                                            value = 'discard',
                                            include_name = False)},
                "f2fs":   {'discard': MOpt(opt_name = 'discard',
                                            value = 'discard',
                                            include_name = False)}
            },
            # "common_mnt_opts"       : ["discard", "nodatacow"],
            "filesystem"            : None,

            ############## FS ##################
            "ext4" : {
                "make_opts": {'-O':['^uninit_bg'], '-b':[4096]}
            },
            "f2fs"  : {"make_opts": {}, 'sysfs':{}},
            "btrfs"  : {"make_opts": {}},


            ############## workload.py workload to age FS ###
            # This is run after mounting the file and before real workload
            # Having this specific aging workload is because we don't want
            # its performance statistics to be recorded.
            "age_workload_class"    : "NoOp",

            # the following config should match the age_workload_class you use
            "aging_config_key"      :None,
            "aging_config" :{
                "generating_func": "self.generate_random_workload",
                # "chunk_count": 100*2**20/(8*1024),
                "chunk_count": 4 * 2**20 / (512 * 1024),
                "chunk_size" : 512 * 1024,
                "iterations" : 1,
                "filename"   : "aging.file",
                "n_col"      : 5   # only for hotcold workload
            },


            ############## workload.py on top of FS #########
            # "workload_class"        : "Simple",
            "workload_class"        : "Synthetic",
            "workload_conf_key"     : "workload_conf",
            "workload_conf" :{
                # "generating_func": "self.generate_hotcold_workload",
                # "generating_func": "self.generate_sequential_workload",
                # "generating_func": "self.generate_backward_workload",
                "generating_func": "self.generate_random_workload",
                # "chunk_count": 100*2**20/(8*1024),
                "chunk_count": 4 * 2**20 / (512 * 1024),
                "chunk_size" : 512 * 1024,
                "iterations" : 1,
                "n_col"      : 5,   # only for hotcold workload
                "filename"   : "test.file"
            },
            "f2fs_gc_after_workload": True,

            ############## LBAGENERATOR  #########
            # if you choose LBAGENERATOR for workload_src, the following will
            # be used
            # "lba_workload_class"    : "Sequential",
            # "lba_workload_class"    : "HotCold",
            # "lba_workload_class"    : "Random",
            "lba_workload_class"    : "Manual",
            "lba_workload_configs"  : {},

            ############# PERF #####################
            "wrap_by_perf" : False,
            "perf" : {
                    "perf_path"         : "perf",
                    "flamegraph_dir"    : None
                    },

            ############# OS #####################
            "linux_version": utils.linux_kernel_version(),
            "n_online_cpus": 'all'
        }

        return confdic

    @property
    def n_pages_per_block(self):
        return self['flash_npage_per_block']

    @property
    def page_size(self):
        return self['flash_page_size']

    @property
    def device_type(self):
        if self['device_path'].startswith("/dev/loop"):
            return 'loop'
        else:
            return 'real'

    def get_device_name_no_num(self):
        return re.search(r'/dev/([a-z]+)[0-9]*',
                self['device_path']).group(1)

    def get_partition_name(self):
        return re.search(r'/dev/([a-z]+[0-9]*)',
                self['device_path']).group(1)


class ConfigNewFlash(Config):
    """
    This config class uses more complex flash configuration with channels,
    chips, packages...
    """
    def __init__(self, confdic = None):
        super(ConfigNewFlash, self).__init__(confdic)

        flash_config = self.flash_default()
        self['flash_config'] = flash_config

        # remove duplication
        del self["flash_page_size"]
        del self["flash_npage_per_block"]
        del self["flash_num_blocks"]

    @property
    def n_pages_per_plane(self):
        return self['flash_config']['n_pages_per_block'] * \
                self['flash_config']['n_blocks_per_plane']

    @property
    def n_pages_per_chip(self):
        return self.n_pages_per_plane * self['flash_config']['n_planes_per_chip']

    @property
    def n_pages_per_package(self):
        return self.n_pages_per_chip * \
            self['flash_config']['n_chips_per_package']

    @property
    def n_blocks_per_channel(self):
        return self['flash_config']['n_blocks_per_plane'] * \
                self['flash_config']['n_planes_per_chip'] * \
                self['flash_config']['n_chips_per_package'] * \
                self['flash_config']['n_packages_per_channel']

    @property
    def n_pages_per_channel(self):
        return self.n_blocks_per_channel * \
            self['flash_config']['n_pages_per_block']

    @property
    def n_blocks_per_dev(self):
        return self.n_blocks_per_channel * \
            self['flash_config']['n_channels_per_dev']

    @property
    def n_pages_per_block(self):
        return self['flash_config']['n_pages_per_block']

    @property
    def page_size(self):
        return self['flash_config']['page_size']

    @property
    def block_bytes(self):
        return self.page_size * self.n_pages_per_block

    @property
    def n_secs_per_page(self):
        return self['flash_config']['page_size'] / self['sector_size']

    @property
    def n_channels_per_dev(self):
        return self['flash_config']['n_channels_per_dev']

    @n_channels_per_dev.setter
    def n_channels_per_dev(self, value):
        self['flash_config']['n_channels_per_dev'] = value

    def flash_default(self):
        flash_config = {
            # layout info
            "page_size"                : 2*KB,
            "n_pages_per_block"        : 2,
            "n_blocks_per_plane"       : 2048,
            "n_planes_per_chip"        : 2,
            "n_chips_per_package"      : 2,
            "n_packages_per_channel"   : 1,
            "n_channels_per_dev"       : 1,

            # time info
            # TODO: these are fixed numbers, but they are random in real world
            # TODO: Note that the SSD time is different than the flash package time
            # DES flash does not use the following three
            "page_read_time"        : 20*MICROSEC,  # Max
            "page_prog_time"        : 200*MICROSEC, # Typical
            "block_erase_time"      : 1.6*MILISEC, # Typical

            # "t_WC"                  : 45*NANOSEC,
            # "t_RC"                  : 50*NANOSEC,
            "t_WC"                  : 0*NANOSEC,
            "t_RC"                  : 0*NANOSEC,

            "t_R"                   : 20*MICROSEC,
            "t_PROG"                : 200*MICROSEC,
            "t_BERS"                : 1.5*MILISEC
            }
        return flash_config

    def set_flash_num_blocks_by_bytes(self, size_byte):
        """
        This function will only change n_blocks_per_plane
        """
        pagesize = self['flash_config']['page_size']
        n_pages_per_block = self['flash_config']['n_pages_per_block']

        nblocks = size_byte / (pagesize * n_pages_per_block)
        rem = size_byte % (pagesize * n_pages_per_block)
        if rem != 0:
            print 'WARNING: set_flash_num_blocks_by_bytes() cannot set to '\
                'exact bytes. rem:', rem

        # change only n_blocks_per_plane
        fconf = self['flash_config']
        n_blocks_per_plane = nblocks / (fconf['n_planes_per_chip'] * \
            fconf['n_chips_per_package'] * fconf['n_packages_per_channel'] * \
            fconf['n_packages_per_channel'] * fconf['n_channels_per_dev'])
        assert n_blocks_per_plane > 0, 'n_blocks_per_plane must be larger' \
            'than zero. Not it is {}'.format(n_blocks_per_plane)
        fconf['n_blocks_per_plane'] = n_blocks_per_plane

    def byte_to_pagenum(self, offset, force_alignment = True):
        "offset to page number"
        if force_alignment and offset % self['flash_config']['page_size'] != 0:
            raise RuntimeError('offset: {off}, page_size: {ps}'.format(
                off=offset, ps = self['flash_config']['page_size']))
        return offset / self['flash_config']['page_size']

    def total_flash_bytes(self):
        return self.n_pages_per_block * self.n_blocks_per_dev * self.page_size

    def off_size_to_page_list(self, off, size, force_alignment = True):
        if force_alignment:
            assert size % self['flash_config']['page_size'] == 0, \
                'size:{}, page_size:{}'.format(size,
                self['flash_config']['page_size'])
            npages = size / self['flash_config']['page_size']
            start_page = self.byte_to_pagenum(off)

            return range(start_page, start_page+npages)
        else:
            start_page = self.byte_to_pagenum(off, force_alignment = False)
            npages = int(math.ceil(float(size) / \
                    self['flash_config']['page_size']))

            return range(start_page, start_page+npages)

    def off_size_to_page_range(self, off, size, force_alignment = True):
        "The input is in bytes"
        if force_alignment:
            if size % self['flash_config']['page_size'] != 0:
                raise RuntimeError('Not aligned size:{}, page_size:{}'.format(
                    size, self['flash_config']['page_size']))
            npages = size / self['flash_config']['page_size']
            start_page = self.byte_to_pagenum(off)

            return start_page, npages
        else:
            start_page = self.byte_to_pagenum(off, force_alignment = False)
            npages = int(math.ceil(float(size) /
                self['flash_config']['page_size']))

            return start_page, npages

    def sec_ext_to_page_ext(self, sector, count):
        """
        The sector extent has to be aligned with page
        return page_start, page_count
        """
        page = sector / self.n_secs_per_page
        assert sector % self.n_secs_per_page == 0,\
            "sector {} is not multiple of n_secs_per_page {}"\
            .format(sector, self.n_secs_per_page)
        page_count = count / self.n_secs_per_page
        assert count % self.n_secs_per_page == 0, \
            "count {} is not multiple of n_secs_per_page {}"\
            .format(count, self.n_secs_per_page)
        return page, page_count

    def page_ext_to_sec_ext(self, page, count):
        sec = page * self.n_secs_per_page
        sec_count = count * self.n_secs_per_page

        return sec, sec_count

    def total_num_pages(self):
        return self['flash_config']['n_pages_per_block'] *\
            self.n_blocks_per_dev

    def block_off_to_page(self, blocknum, pageoff):
        "convert block number and page offset to page number"
        return blocknum * self['flash_config']['n_pages_per_block'] + pageoff

    def page_to_block(self, pagenum):
        d = {}
        d['blocknum'] = pagenum / self['flash_config']['n_pages_per_block']
        d['pageoffset'] = pagenum % self['flash_config']['n_pages_per_block']
        return d

    def page_to_block_off(self, pagenum):
        "return block, page_offset"
        return pagenum / self['flash_config']['n_pages_per_block'], \
                pagenum % self['flash_config']['n_pages_per_block']

    def block_off_to_page(self, blocknum, pageoff):
        "convert block number and page offset to page number"
        return blocknum * self['flash_config']['n_pages_per_block'] + pageoff

    def block_to_page_range(self, blocknum):
        return blocknum * self['flash_config']['n_pages_per_block'], \
                (blocknum + 1) * self['flash_config']['n_pages_per_block']


class ConfigNotForceAlign(ConfigNewFlash):
    def sec_ext_to_page_ext(self, sector, count):
        """
        The sector extent has to be aligned with page
        return page_start, page_count
        """
        page = sector / self.n_secs_per_page
        page_end = (sector + count) / self.n_secs_per_page
        page_count = page_end - page
        if (sector + count) % self.n_secs_per_page != 0:
            page_count += 1
        return page, page_count


class ConfigNCQFTL(ConfigNewFlash):
    def __init__(self, confdic = None):
        super(ConfigNCQFTL, self).__init__(confdic)

        self['SSDFramework'] = {'ncq_depth': 32,
                                'data_cache_max_n_entries': 4096
                                }
        self['process_queue_depth'] = 32
        self['simulator_enable_interval'] = False

    def ssd_ncq_depth(self):
        return self['SSDFramework']['ncq_depth']

    def page_read_time(self):
        return self['flash_config']['t_R']

    def page_prog_time(self):
        return self['flash_config']['t_PROG']

    def block_erase_time(self):
        return self['flash_config']['t_BERS']



