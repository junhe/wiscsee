#!/usr/bin/env python
import json
from common import *

conf = load_json('config')

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

    cmd = ["mount", "-t", "ext4", "-o", "discard", devname, mountpoint]
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


class Filesystem(object):
    def __init__(self, mountpoint, dev):
        self.mountpoint = mountpoint
        self.dev = dev

    def make(self, opt=None):
        raise NotImplementedError

    def mount(self, opt=None):
        raise NotImplementedError

    def is_mounted(self):
        return isMounted(self.dev)

    def umount(self):
        return umountFS(self.mountpoint)

class F2fs(Filesystem):
    def make(self, opt=None):
        if opt == None:
            opt = ''
        return shcmd('mkfs.f2fs {opt} {dev}'.format(
            opt=opt, dev = self.dev))

    def mount(self, opt=None):
        if opt == None:
            opt = ''
        return shcmd('mount -t f2fs {dev} {mp}'.format(
            dev = self.dev, mp = self.mountpoint))

# f2fs = F2fs('/mnt/fsonloop', '/dev/loop0')
# print f2fs.umount()
# print f2fs.make()
# print f2fs.mount()

def prepare_loop():
    make_loop_device(conf["loop_path"], conf["tmpfs_mount_point"], 4096, img_file=None)

