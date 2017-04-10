import subprocess
import os
import re
import itertools

"""
Example:
dump_extents_of_a_file('/dev/loop0', 'datafile')
"""

def dump_extents_of_a_file(devname, filepath):
    """
    this is only for ext4
    """
    cmd = ['debugfs', devname, '-R', 'dump_extents "' + filepath + '"']
    proc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    proc.wait()

    lines = proc.stdout.readlines()
    return ''.join(lines)


def parse_dump_extents_output(output_text):
    ret_list = []
    header = ["Level_index", "Max_level",
             "Entry_index", "N_Entry",
             "Logical_start", "Logical_end",
             "Physical_start", "Physical_end",
             "Length", "Flags"]

    lines = output_text.split('\n')
    for line in lines:
        if "Level" in line or "debugfs" in line or len(line.strip()) == 0:
            continue

        line = re.sub(r'[/\-]', " ", line)
        tokens = line.split()
        if len(tokens) == 8:
            # there is no physical end
            tokens.insert(7, tokens[6]) #TODO: this is dangerous

        d = {}
        for i in range(9):
            d[ header[i] ] = int(tokens[i])

        if len(tokens) == 10:
            d["Flags"] = tokens[9]
        else:
            d["Flags"] = "NA"

        ret_list.append(d)

    return ret_list


def _add_file_path(extents, file_path):
    for extent in extents:
        extent['file_path'] = file_path

    return extents


def get_extents_of_dir(dirpath, dev_path):
    """
    Example:
    extents = get_extents_of_dir(dirpath = '/mnt/fsonloop', dev_path = '/dev/sdc1')
    """
    all_extents = []
    for root, dirs, files in os.walk(dirpath, topdown=False):
        for name in itertools.chain(files, dirs):
            rel_dir_path = os.path.relpath(root, dirpath)
            rel_path = os.path.join(rel_dir_path, name)

            out_txt = dump_extents_of_a_file(dev_path, rel_path)
            file_extents = parse_dump_extents_output(out_txt)
            _add_file_path(file_extents, rel_path)
            all_extents.extend(file_extents)

    return all_extents



