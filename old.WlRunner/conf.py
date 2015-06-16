import os
from common import load_json

filedir = os.path.dirname(os.path.abspath(__file__))
config = load_json(os.path.join(filedir, 'config'))

# if you change anything in config and reload this file, you change
# may be lost.
# config["blkparse_result_path"] =

def get_blkparse_result_path():
    return os.path.join(config['result_dir'], 'blkparse-output.txt')

def get_blkparse_result_table_path():
    return os.path.join(config['result_dir'], 'blkparse-output-table.txt')

def get_ftlsim_events_output_path():
    "This is the path to output parsed blkparse results"
    return os.path.join(config['result_dir'], 'blkparse-events-for-ftlsim.txt')

