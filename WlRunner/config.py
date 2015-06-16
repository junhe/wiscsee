import json
import os

class Config(dict):
    def show(self):
        print self

    def load_from_dict(self, dic):
        super(Config, self).clear()
        super(Config, self).__init__(dic)

    def load_from_json_file(self, file_path):
        decoded = json.load(open(file_path, 'r'))
        self.load_from_dict(decoded)

    def get_blkparse_result_path(self):
        return os.path.join(self['result_dir'], 'blkparse-output.txt')

    def get_blkparse_result_table_path(self):
        return os.path.join(self['result_dir'], 'blkparse-output-table.txt')

    def get_ftlsim_events_output_path(self):
        "This is the path to output parsed blkparse results"
        return os.path.join(self['result_dir'], 'blkparse-events-for-ftlsim.txt')

