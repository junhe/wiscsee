import os
import subprocess
import itertools
import shlex
import json

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


def parameter_combinations(parameter_dict):
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


def run_and_get_output(cmd, shell = False):
    output = []
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE)
    p.wait()

    return p.stdout.readlines()


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


def linux_kernel_version():
    kernel_ver = run_and_get_output('uname -r')[0].strip()
    return kernel_ver


def adjust_width(s, width = 32):
    return s.rjust(width)


def table_to_str(table, adddic=None, sep=';', width=32):
    """
    table is of format: [
                    {'col1':data, 'col2':data, ..},
                    {'col1':data, 'col2':data, ..},
                    {'col1':data, 'col2':data, ..},
                    ]
    output is:
        col1   col2   col3 ..
        data   data   data ..

    """
    if len(table) == 0:
        return ""

    tablestr = ''
    colnames = table[0].keys()
    if adddic != None:
        colnames += adddic.keys()
    colnamestr = sep.join([adjust_width(s, width=width) for s in colnames]) + '\n'
    tablestr += colnamestr
    for row in table:
        if adddic != None:
            rowcopy = dict(row.items() + adddic.items())
        else:
            rowcopy = row
        rowstr = [rowcopy[k] for k in colnames]
        rowstr = [adjust_width(str(x), width=width) for x in rowstr]
        rowstr = sep.join(rowstr) + '\n'
        tablestr += rowstr

    return tablestr


def _tarfilename(tarname):
    return '{}.tar.xz'.format(tarname)


def download_kernel(dirpath, tarname):
    """
    e.g. tarname='linux-4.5.4'
    """
    with cd(dirpath):
        tarfile = _tarfilename(tarname)
        shcmd("wget https://www.kernel.org/pub/linux/kernel/v4.x/{}"\
                .format(tarfile))
        shcmd("tar xf {}".format(tarfile))


def read_byte_range(filepath, start, size):
    f = open(filepath, 'rb')
    f.seek(start)

    data = []
    for i in range(size):
        byte = f.read(1)
        value = ord(byte)
        data.append(value)

    f.close()

    return data

def display_binary(data):
    for v in data:
        print '{v}({h})'.format(v=v, h=hex(v)),


def run_cmd_on_nodes(cmd, nodes, sync, id_map, do_not_run=False):
    """
    example id_map:
        {0:'node-0', 1:'node-1'}

    example usage:
        run_cmd_on_nodes(cmd='hostname', nodes=[25], sync=True, id_map=table)
    """
    procs = {}
    for node_id in nodes:
        print '----------', node_id, '----------'
        p = run_cmd_on_node(cmd, node_id, sync, id_map, do_not_run)
        if not p is None:
            procs[node_id] = p

    # wait
    for node_id, p in procs.items():
        ret = p.wait()
        print 'Node', node_id, 'returned', ret


def run_cmd_on_node(cmd, node_id, sync, id_map, do_not_run):
    cmd = "ssh {host} '{cmd}'".format(host=id_map[node_id], cmd=cmd)

    if do_not_run is True:
        print cmd
        return None

    if sync is True:
        print 'sync', cmd
        shcmd(cmd)
        return None
    else:
        print 'async', cmd
        p = subprocess.Popen(cmd, shell=True)
        return p


########################################################
# table = [
#           {'col1':data, 'col2':data, ..},
#           {'col1':data, 'col2':data, ..},
#           ...
#         ]
def table_to_file(table, filepath, adddic=None, width=32):
    'save table to a file with additional columns'
    with open(filepath, 'w') as f:
        if len(table) == 0:
            return
        f.write( table_to_str(table, adddic=adddic, width=width) )

def drop_caches():
    cmd = "echo 3 > /proc/sys/vm/drop_caches"
    subprocess.call(cmd, shell=True)







