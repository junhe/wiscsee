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
    "n_planes_per_die"         : 4,
    "n_dies_per_package"       : 1,
    "n_packages_per_channel"   : 1,
    "n_channels"               : 1,

    # time info
    # TODO: these are fixed numbers, but they are random in real world
    "page_read_time"        : 25*USEC,  # Max
    "page_prog_time"        : 200*USEC, # Typical
    "block_erase_time"      : 1.6*MSEC, # Typical
    }
