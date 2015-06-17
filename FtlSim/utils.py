import json
import os

def load_json(fpath):
    decoded = json.load(open(fpath, 'r'))
    return decoded

def prepare_dir_for_path(path):
    "create parent dirs for path if necessary"
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)


