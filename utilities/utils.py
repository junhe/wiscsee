import itertools
import json
import random
import argparse
import re
import subprocess
import os
import sys
import string
import shlex
import time
import glob
from time import localtime, strftime

def shcmd(cmd, ignore_error=False):
    print 'Doing:', cmd
    ret = subprocess.call(cmd, shell=True)
    print 'Returned', ret, cmd
    if ignore_error == False and ret != 0:
        raise RuntimeError("Failed to execute {}. Return code:{}".format(
            cmd, ret))
    return ret

def run_and_get_output(cmd, shell = False):
    output = []
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE)
    p.wait()

    return p.stdout.readlines()

class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

########################################################
# table = [
#           {'col1':data, 'col2':data, ..},
#           {'col1':data, 'col2':data, ..},
#           ...
#         ]
def table_to_file(table, filepath, adddic=None):
    'save table to a file with additional columns'
    with open(filepath, 'w') as f:
        if len(table) == 0:
            return
        colnames = table[0].keys()
        if adddic != None:
            colnames += adddic.keys()
        colnamestr = ';'.join(colnames) + '\n'
        f.write(colnamestr)
        for row in table:
            if adddic != None:
                rowcopy = dict(row.items() + adddic.items())
            else:
                rowcopy = row
            rowstr = [rowcopy[k] for k in colnames]
            rowstr = [str(x) for x in rowstr]
            rowstr = ';'.join(rowstr) + '\n'
            f.write(rowstr)

def adjust_width(s, width = 32):
    return s.rjust(width)

def table_to_str(table, adddic = None, sep = ';'):
    if len(table) == 0:
        return None

    tablestr = ''
    colnames = table[0].keys()
    if adddic != None:
        colnames += adddic.keys()
    colnamestr = sep.join([adjust_width(s) for s in colnames]) + '\n'
    tablestr += colnamestr
    for row in table:
        if adddic != None:
            rowcopy = dict(row.items() + adddic.items())
        else:
            rowcopy = row
        rowstr = [rowcopy[k] for k in colnames]
        rowstr = [adjust_width(str(x)) for x in rowstr]
        rowstr = sep.join(rowstr) + '\n'
        tablestr += rowstr

    return tablestr

def load_json(fpath):
    decoded = json.load(open(fpath, 'r'))
    return decoded

def dump_json(dic, file_path):
    with open(file_path, "w") as f:
        json.dump(dic, f, indent=4)

def prepare_dir_for_path(path):
    "create parent dirs for path if necessary"
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

def prepare_dir(dirpath):
    "create parent dirs for path if necessary"
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

def ParameterCombinations(parameter_dict):
    """
    Get all the cominbation of the values from each key
    http://tinyurl.com/nnglcs9
    Input: parameter_dict={
                    p0:[x, y, z, ..],
                    p1:[a, b, c, ..],
                    ...}
    Output: [
             {p0:x, p1:a, ..},
             {..},
             ...
            ]
    """
    d = parameter_dict
    return [dict(zip(d, v)) for v in itertools.product(*d.values())]

def debug_decor(function):
    def wrapper(*args, **kwargs):
        ret = function(*args, **kwargs)
        print function.__name__, args, kwargs, 'ret:', ret
        return ret
    return wrapper

def breakpoint():
    import pdb; pdb.set_trace()


def linux_kernel_version():
    kernel_ver = run_and_get_output('uname -r')[0].strip()
    return kernel_ver

def set_vm(name, value):
    filepath = os.path.join("/proc/sys/vm/", name)
    with open(filepath, 'w') as f:
        f.write(str(value))

def set_vm_default():
    set_vm("dirty_background_bytes", 0)
    set_vm("dirty_background_ratio", 10)
    set_vm("dirty_bytes", 0)
    set_vm("dirty_ratio", 20)
    set_vm("dirty_expire_centisecs", 3000)
    set_vm("dirty_writeback_centisecs", 500)

def set_linux_ncq_depth(devname, depth):
    filepath = "/sys/block/{}/device/queue_depth".format(devname)
    shcmd("echo {depth} > {filepath}".format(
        depth = depth, filepath = filepath))

def get_linux_ncq_depth(devname):
    filepath = "/sys/block/{}/device/queue_depth".format(devname)
    with open(filepath, 'r') as f:
        line = f.readline()
        return int(line.strip())

def set_linux_io_scheduler(devname, scheduler_name):
    filepath = "/sys/block/{}/queue/scheduler".format(devname)
    shcmd("echo {scheduler_name} > {filepath}".format(
        scheduler_name = scheduler_name,
        filepath = filepath))

def get_linux_io_scheduler(devname):
    filepath = "/sys/block/{}/queue/scheduler".format(devname)
    with open(filepath, 'r') as f:
        line = f.readline()
        return re.search(r'\[(\w+)\]', line).group(1)

def runtime_update(conf):
    """
    This function has to be called before running each treatment.
    """
    conf['time'] = time.strftime("%m-%d-%H-%M-%S", time.localtime())
    conf['hash'] = hash(str(conf))
    if conf.has_key('filesystem') and conf['filesystem'] != None:
        fs = str(conf['filesystem'])
    else:
        fs = 'fsnotset'
    conf['result_dir'] = "{targetdir}/{expname}/{subexpname}-{unique}".format(
        targetdir = conf['targetdir'], expname = conf['expname'],
        subexpname = conf['subexpname'],
        unique = '-'.join((fs, conf['time'], str(conf['hash']))))


def choose_exp_metadata(default_conf, interactive = True):
    """
    This function will return a dictionary containing a few things relating
    to result dir. You can update the experiment configuration by
    confdic.update(return of this function)

    default_conf is readonly in this function.

    Usage: Call this function for once to get a dictionary with things needing
    to be updated. Use the returned dictionary multiple times to update
    experimental config. Note that conf['result_dir'] still needs to be updated
    later for each experiment.
    """
    conf = {}
    result_dir = '/tmp/results'
    # result_dir = '/users/jhe/results'
    # result_dir = '/mnt/ramdisk/results'
    if interactive == True:
        toresult = raw_input('Save this experiments to {}? (y/n)'.format(result_dir))
    else:
        toresult = 'n'
    if toresult.lower() == 'y':
        targetdir = result_dir
        expname = raw_input('Enter expname ({}):'.format(default_conf['expname']))
        if expname.strip() != '':
            conf['expname'] = expname
        else:
            conf['expname'] = default_conf['expname']

        subexpname = raw_input('Enter subexpname ({}):'.format(
            default_conf['subexpname']))
        if subexpname.strip() != '':
            conf['subexpname'] = subexpname
        else:
            conf['subexpname'] = default_conf['subexpname']
    else:
        targetdir = '/tmp/resulttmp'
        conf['expname'] = default_conf['expname']
        conf['subexpname'] = default_conf['subexpname']

    conf['targetdir'] = targetdir
    return conf

def set_exp_metadata(conf, save_data, expname, subexpname):
    if save_data == True:
        targetdir = '/tmp/results'
    else:
        targetdir = '/tmp/tmpresults'

    conf['targetdir'] = targetdir

    conf['expname'] = expname
    conf['subexpname'] = subexpname

def chain_items_as_str(iterator):
    return '.'.join([str(x) for x in iterator])

def chain_items_as_filename(iterator):
    s = chain_items_as_str(iterator)
    return str_as_filename(s)

def str_as_filename(s):
    """
    valid_chars
    '-_.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    """
    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in s if c in valid_chars)


def assert_multiple(n, divider):
    "n is multiple of divider"
    assert n % divider == 0, "{} is not mutliple of {}".format(n, divider)


def get_expname():
    ret = raw_input("Enter expname (default-expname):")
    if ret == "":
        return "default-expname"
    else:
        return ret

def str_as_filename(s):
    """
    valid_chars
    '-_.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    """
    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in s if c in valid_chars)

def drop_caches():
    subprocess.call("sync", shell=True)
    cmd = "echo 3 > /proc/sys/vm/drop_caches"
    subprocess.call(cmd, shell=True)

def invoke_f2fs_gc(mountpoint, sync):
    binpath = './bin/forcef2fsgc'
    if not os.path.exists(binpath):
        raise RuntimeError("{} does not exist. To compile, do 'make f2fsgc'"\
                .format(binpath))

    cmd = [binpath, mountpoint, str(sync)]
    ret = subprocess.call(cmd)
    return ret



