import os

from utilities.utils import *


class Workflow(object):
    def __init__(self, conf):
        self.conf = conf

    def save_conf(self):
        confpath = os.path.join(self.conf['result_dir'], 'config.json')
        prepare_dir_for_path(confpath)
        self.conf.dump_to_file(confpath)
