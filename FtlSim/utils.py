import json

def load_json(fpath):
    decoded = json.load(open(fpath, 'r'))
    return decoded


