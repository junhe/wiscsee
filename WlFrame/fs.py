#!/usr/bin/env python
import json
from common import *

conf = load_json('config')
print conf

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

# def ext4_create_on_loop():
    # makeLoopDevice(conf["loop_path"], conf["tmpfs_mount_point"], 4096, img_file=None)
    # ext4_make(conf["loop_path"], blocksize=4096, makeopts=None)
    # ext4_mount(devname=conf["loop_path"], mountpoint=conf["fs_mount_point"])

def ext4_make_simple():
    ret = ext4_make(conf["loop_path"], blocksize=4096, makeopts=None)
    if ret != 0:
        print 'error in ext4_make_simple()'
        exit(1)

def ext4_mount_simple():
    ret = ext4_mount(devname=conf["loop_path"], mountpoint=conf["fs_mount_point"])
    if ret != 0:
        print 'error in ext4_mount_simple()'
        exit(1)

def prepare_loop():
    make_loop_device(conf["loop_path"], conf["tmpfs_mount_point"], 4096, img_file=None)

