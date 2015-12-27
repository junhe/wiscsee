from commons import *

"""
Rule of Thumb: when passing this config to class init,
save all the values to be used to class attributes in the __init__().
This way you can easily find out what is used in the class later.
"""

flash_config = {
    # layout info
    "page_size"                : 2*KB,
    "n_pages_per_block"        : 64,
    "n_blocks_per_plane"       : 2048,
    "n_planes_per_chip"        : 4,
    "n_chips_per_package"      : 2,
    "n_packages_per_channel"   : 1,
    "n_channels_per_dev"       : 1,

    # time info
    # TODO: these are fixed numbers, but they are random in real world
    # TODO: Note that the SSD time is different than the flash package time
    "page_read_time"        : 25*USEC,  # Max
    "page_prog_time"        : 200*USEC, # Typical
    "block_erase_time"      : 1.6*MSEC, # Typical
    }

def calc_and_cache(conf):
    n_pages_per_plane = conf['n_pages_per_block'] * conf['n_blocks_per_plane']
    n_pages_per_chip = n_pages_per_plane * conf['n_planes_per_chip']
    n_pages_per_package = n_pages_per_chip * conf['n_chips_per_package']

    conf['n_pages_per_plane'] = n_pages_per_plane
    conf['n_pages_per_chip'] = n_pages_per_chip
    conf['n_pages_per_package'] = n_pages_per_package

calc_and_cache(flash_config)


