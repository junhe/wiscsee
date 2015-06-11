# This a framework for create file systems, manipulate blktraces, and parse
# blktraces
import time

from common import *
import fs
import pyblktrace as bt
from conf import config

def main():
    fs.prepare_loop()

    trace_proc = bt.start_blktrace_on_bg(dev=config['loop_path'],
        resultpath=config['blkparse_result_path'])

    fs.ext4_make_simple()
    fs.ext4_mount_simple()

    # run workload here
    shcmd("sync")
    shcmd("cp -r /boot {}".format(config["fs_mount_point"]))
    shcmd("rm -r {}/*".format(config["fs_mount_point"]))
    shcmd("sync")

    # bt.stop_blktrace_on_bg(trace_proc)
    # try to kill by shell
    shcmd('pkill blkparse', ignore_error=True)
    shcmd('pkill blktrace', ignore_error=True)
    shcmd('sync')

    # parse the result here
    bt.blkparse_to_table_file(config['blkparse_result_path'],
        config['final_table_path'])

if __name__ == '__main__':
    main()

