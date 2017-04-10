import collections
from pyreuse.helpers import *

NOVALUE, HIDE_ATTR = 'NOVALUE', 'HIDE_ATTR'

class JobConfig(collections.OrderedDict):
    """
    It manages an in-memeory representation of the FIO. The core
    is a ordered dict
    Format:
        { "global": {
            "size": xxx,
            "xxx" : xxx,
            },
          "job1": {
            "xxx" : xxx,
            "xxx" : xxx
            }
        }
    """
    def append_section(self, section_name, section_dict):
        self[section_name] = section_dict

    def remove_section(self, section_name):
        del self[section_name]

    def update(self, section_name, attr_name, attr_value):
        self[section_name][attr_name] = attr_value

    def get(self, section_name, attr_name):
        return self[section_name][attr_name]

    def as_ordered_dict(self):
        return self

    def __str__(self):
        lines = []
        for section_name, section_dict in self.items():
            lines.append("[{}]".format(section_name))

            for attr_name, attr_value in section_dict.items():
                if attr_value == NOVALUE:
                    lines.append(attr_name)
                elif attr_value == HIDE_ATTR:
                    continue
                else:
                    lines.append("{}={}".format(attr_name, attr_value))

        return '\n'.join(lines)

    def save(self, filepath):
        prepare_dir_for_path(filepath)
        with open(filepath, 'w') as f:
            f.write(str(self))


class Fio(object):
    def __init__(self, conf_path, result_dir, to_json = True):
        self.conf_path = conf_path
        self.result_dir = result_dir
        self.result_path = os.path.join(result_dir, 'fio.result.json')
        self.to_json = to_json

    def parse_results(self):
        d = load_json(self.result_path)
        table = parse_json_results(d)
        table_to_file(table, self.result_path + '.parsed')

    def run(self):
        if self.to_json == True:
            prepare_dir_for_path(self.result_path)
            fio_cmd = "fio {} --output-format=json --output {}".format(
                self.conf_path, self.result_path)
        else:
            fio_cmd = "fio {}".format(self.conf_path)

        with cd(self.result_dir):
            shcmd(fio_cmd)

        if self.to_json:
            self.parse_results()


def parse_json_results(d):
    """
    The input d is the json dictionary of FIO output.
    All job perfs will be put into table and returned.
    """
    table = []
    for job in d['jobs']:
        my_dict = {
                  'jobname':  job['jobname'],
                  'read_bw':  job['read']['bw'],
                  'read_iops': job['read']['iops'],
                  'read_iobytes': job['read']['io_bytes'],
                  'read_runtime': job['read']['runtime'],

                  'write_bw':  job['write']['bw'],
                  'write_iops': job['write']['iops'],
                  'write_iobytes': job['write']['io_bytes'],
                  'write_runtime': job['write']['runtime'],
                }
        table.append(my_dict)

    return table





