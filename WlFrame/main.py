# This a framework for create file systems, manipulate blktraces, and parse
# blktraces
import time

from common import *
import fs
import pyblktrace as bt
from conf import config

def main():
    fs.prepare_loop()

    trace_proc = bt.start_blktrace_on_bg(dev='/dev/loop0', resultdir='/tmp/',
        basename='tmptrace')

    fs.ext4_make_simple()
    fs.ext4_mount_simple()

    # run workload here
    shcmd("cp /boot/vmlinuz-3.16.0-30-generic {}".format(config["fs_mount_point"]))

    bt.stop_blktrace_on_bg(trace_proc)

    # parse the result here

if __name__ == '__main__':
    main()

