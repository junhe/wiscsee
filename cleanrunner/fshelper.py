#!/usr/bin/env python
import json
import os
import re
import subprocess

def umountFS(mountpoint):
    cmd = ["umount", mountpoint]
    p = subprocess.Popen(cmd)
    p.wait()
    return p.returncode

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
    # makeLoopDevice(config["loop_path"], config["tmpfs_mount_point"], 4096, img_file=None)
    # ext4_make(config["loop_path"], blocksize=4096, makeopts=None)
    # ext4_mount(devname=config["loop_path"], mountpoint=config["fs_mount_point"])

def ext4_make_simple():
    ret = ext4_make(config["loop_path"], blocksize=4096, makeopts=None)
    if ret != 0:
        print 'error in ext4_make_simple()'
        exit(1)

def ext4_mount_simple():
    ret = ext4_mount(devname=config["loop_path"], mountpoint=config["fs_mount_point"])
    if ret != 0:
        print 'error in ext4_mount_simple()'
        exit(1)

def mkLoopDevOnFile(devname, filepath):
    cmd = ['losetup', devname, filepath]
    cmd = [str(x) for x in cmd]
    print " ".join(cmd), "......"
    proc = subprocess.Popen(cmd)
    proc.wait()

    return proc.returncode

def delLoopDev(devname):
    cmd = ['losetup', '-d', devname]
    cmd = [str(x) for x in cmd]
    print " ".join(cmd), "......"
    proc = subprocess.Popen(cmd)
    proc.wait()

    return proc.returncode

def isMounted(name):
    "only check is a name is in mounted list"
    name = name.rstrip('/')
    print "isMounted: name:", name
    with open('/etc/mtab', 'r') as f:
        for line in f:
            #print "line:", line,
            line = " " + line + " " # a hack
            if re.search(r'\s'+name+r'\s', line):
                #print " YES"
                return True
            #print " NO"
    return False

def isLoopDevUsed(path):
    cmd = ['losetup','-f']
    proc = subprocess.Popen(cmd,
            stdout=subprocess.PIPE)
    proc.wait()

    outstr = proc.communicate()[0]
    outstr = outstr.strip()
    if outstr > path:
        return True
    else:
        return False

def umountFS(mountpoint):
    cmd = ["umount", mountpoint]
    p = subprocess.Popen(cmd)
    p.wait()
    return p.returncode

def make_loop_device(devname, tmpfs_mountpoint, sizeMB, img_file=None):
    "size is in MB. The tmpfs for this device might be bigger than sizeMB"
    if not devname.startswith('/dev/loop'):
        raise RuntimeError('you are requesting to create loop device on a non-loop device path')

    if not os.path.exists(tmpfs_mountpoint):
        os.makedirs(tmpfs_mountpoint)

    # umount the FS mounted on loop dev
    if isMounted(devname):
        if umountFS(devname) != 0:
            raise RuntimeError("unable to umount {}".format(devname))
        else:
            print devname, 'umounted'
    else:
        print devname, "is not mounted"

    # delete the loop device
    if isLoopDevUsed(devname):
        if delLoopDev(devname) != 0:
            raise RuntimeError("!!!!!!!!!!!!! Failed to delete loop device")
        else:
            print devname, 'is deleted'
    else:
        print devname, "is not in use"


    # umount the tmpfs the loop device is on
    if isMounted(tmpfs_mountpoint):
        if umountFS(tmpfs_mountpoint) != 0:
            raise RuntimeError("unable to umount tmpfs at {}".format(tmpfs_mountpoint))
        print tmpfs_mountpoint, "umounted"
    else:
        print tmpfs_mountpoint, "is not mounted"


    mountTmpfs(tmpfs_mountpoint, int(sizeMB*1024*1024*1.1))
    imgpath = os.path.join(tmpfs_mountpoint, "disk.img")
    if img_file == None:
        mkImageFile(imgpath, sizeMB)
    else:
        cmd = ['cp', img_file, imgpath]
        subprocess.call(cmd)

    ret = mkLoopDevOnFile(devname, imgpath)
    if ret != 0:
        raise RuntimeError("Failed at losetup")

def mkImageFile(filepath, size):
    "size is in MB"
    cmd = ['truncate', '-s', str(size*1024*1024), filepath]
    print " ".join(cmd), "......"
    proc = subprocess.Popen(cmd)
    proc.wait()
    return proc.returncode

def mountTmpfs(mountpoint, size):
    if not os.path.exists(mountpoint):
        os.makedirs(mountpoint)
    cmd = ['mount', '-t', 'tmpfs',
           '-o', 'size='+str(size), 'tmpfs', mountpoint]
    cmd = [str(x) for x in cmd]
    print " ".join(cmd), "......"
    proc = subprocess.Popen(cmd)
    proc.wait()

    return proc.returncode


# def prepare_loop():
    # make_loop_device(config["loop_path"], config["tmpfs_mount_point"], 4096, img_file=None)

