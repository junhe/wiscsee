# This a framework for create file systems, manipulate blktraces, and parse
# blktraces
import time

from common import *
import fs
import pyblktrace as bt
import config

def prepare_dev():
    fs.prepare_loop()

def start_blktrace():
    return bt.start_blktrace_on_bg(dev=config['loop_path'],
        resultpath=config.get_blkparse_result_path())

def stop_blktrace():
    bt.stop_blktrace_on_bg()

def prepare_fs():
    if config['filesystem'] == 'ext4':
        fs.ext4_make_simple()
        fs.ext4_mount_simple()
    elif config['filesystem'] == 'f2fs':
        f2fs = fs.F2fs(config['fs_mount_point'], config['loop_path'])
        f2fs.make()
        f2fs.mount()


def run_workload():
    # run workload here
    shcmd("sync")
    shcmd("cp -r /boot {}".format(config["fs_mount_point"]))
    shcmd("rm -r {}/*".format(config["fs_mount_point"]))
    shcmd("sync")

def process_data():
    bt.blkparse_to_files(config.get_blkparse_result_path(),
        config.get_blkparse_result_table_path())

def main():
    try:
        prepare_dev()
        start_blktrace()
        try:
            prepare_fs()
            run_workload()
        finally:
            stop_blktrace()
    finally:
        process_data()

def run():
    main()
    with open(config.get_ftlsim_events_output_path(), 'r') as f:
        for line in f:
            yield line.strip()

if __name__ == '__main__':
    main()

