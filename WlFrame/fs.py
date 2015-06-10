#!/usr/bin/env python
import json
from common import *

conf = load_json('config')
print conf

exit(1)

def ext4_make(devname, blocksize=4096, makeopts=None):

    if makeopts == None:
        cmd = ["mkfs.ext4",
               "-b", blocksize,
               "-O", "^has_journal,extent,huge_file,flex_bg,uninit_bg,dir_nlink,extra_isize",
               devname]
    else:
        cmd = ["mkfs.ext4",
               "-b", blocksize]
        cmd.extend(makeopts)
        cmd.extend([devname])

    cmd = [str(x) for x in cmd]
    p = subprocess.Popen(cmd)
    p.wait()
    print "makeExt4:", p.returncode
    return p.returncode

def ext4_mount(devname, mountpoint):
    if not os.path.exists(mountpoint):
        os.makedirs(mountpoint)

    cmd = ["mount", "-t", "ext4", devname, mountpoint]
    p = subprocess.Popen(cmd)
    p.wait()
    print "mountExt4:", p.returncode
    return p.returncode

def ext4_create_on_loop():
    makeLoopDevice(loop_path, tmpfs_mount_point, 4096, img_file=None)
    ext4_make(loop_path, blocksize=4096, makeopts=None)
    ext4_mount(devname=loop_path, mountpoint=fs_mount_point)

def ext4_make_and_mount():
    ext4_make(config.loop_path, blocksize=4096, makeopts=None)
    ext4_mount(devname="/dev/loop0", mountpoint="/mnt/fsonloop")

def prepare_loop():
    makeLoopDevice("/dev/loop0", "/mnt/tmpfs", 4096, img_file=None)

