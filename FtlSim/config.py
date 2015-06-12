import json
import os

from common import load_json

# Wait for the main or others to load the confdic
confdic = None

def load_from_json(json_path):
    global confdic
    confdic = load_json(json_path)

def load_from_dict(dic):
    global confdic
    confdic = dic

# flash_page_size       = 4096
# flash_npage_per_block = 128
# flash_num_blocks      = 8*2**30 / (flash_page_size * flash_npage_per_block)

# for debugging
flash_page_size       = 4096
flash_npage_per_block = 16
flash_num_blocks      = 512


# directmap pagemap blockmap hybridmap
# ftl_type = 'pagemap'
# ftl_type = 'blockmap'
ftl_type = 'hybridmap'

# for hybrid mapping
# Note that log_block_ratio + data_block_ratio does not necessary
# equal to 1.0. But it must be less than 1.0
high_log_block_ratio = 0.4   # ratio of log block over all flash blocks
high_data_block_ratio = 0.4  # ratio of data blocks over all flash blocks
log_block_upperbound_ratio = 0.5 # to limit RAM usage for the page mapping
assert high_log_block_ratio < log_block_upperbound_ratio

# for general
verbose_level = 1
output_target = 'file' # stdout or file
# output_target = 'stdout' # stdout or file
output_dir = './'

def get_output_file_path():
    return os.path.join(output_dir, 'ftlsim.out')

