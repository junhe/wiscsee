import itertools
import json
import random
import argparse
import re
import subprocess
import os
import sys
import shlex
import time
import glob
from time import localtime, strftime

def shcmd(cmd, ignore_error=False):
    print 'Doing:', cmd
    ret = subprocess.call(cmd, shell=True)
    print 'Returned', ret, cmd
    if ignore_error == False and ret != 0:
        exit(ret)
    return ret

class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

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
        print 'you are requesting to create loop device on a non-loop device path'
        exit(1)

    if not os.path.exists(tmpfs_mountpoint):
        os.makedirs(tmpfs_mountpoint)

    # umount the FS mounted on loop dev
    if isMounted(devname):
        if umountFS(devname) != 0:
            print "!!!!!!!!!!!!!!!!!! unable to umount", devname
            exit(1)
        else:
            print devname, 'umounted'
    else:
        print devname, "is not mounted"

    # delete the loop device
    if isLoopDevUsed(devname):
        if delLoopDev(devname) != 0:
            print "!!!!!!!!!!!!! Failed to delete loop device"
            exit(1)
        else:
            print devname, 'is deleted'
    else:
        print devname, "is not in use"


    # umount the tmpfs the loop device is on
    if isMounted(tmpfs_mountpoint):
        if umountFS(tmpfs_mountpoint) != 0:
            print "!!!!!!!!!!!!!!!!!! unable to umount tmpfs at", tmpfs_mountpoint
            exit(1)
        print tmpfs_mountpoint, "umounted"
    else:
        print tmpfs_mountpoint, "is not mounted"


    mountTmpfs(tmpfs_mountpoint, int(sizeMB*1024*1024*1.1))
    imgpath = os.path.join(tmpfs_mountpoint, "disk.img")
    if img_file == None:
        mkImageFile(imgpath, sizeMB)
    else:
        cmd = ['cp', img_file, imgpath]
        print 'doing...', cmd
        subprocess.call(cmd)

    ret = mkLoopDevOnFile(devname, imgpath)
    if ret != 0:
        print "!!!!!!!!!!!!!!!!! failed at losetup"
        exit(1)

def mkImageFile(filepath, size):
    "size is in MB"
    #cmd = ['dd', 'if=/dev/zero', 'of='+filepath,
           #'bs=1M', 'count='+str(size)]
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

def load_json(fpath):
    decoded = json.load(open(fpath, 'r'))
    return decoded

