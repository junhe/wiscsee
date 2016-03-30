import collections
from utilities import utils

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
        utils.prepare_dir_for_path(filepath)
        with open(filepath, 'w') as f:
            f.write(str(self))


def parse_results(d):
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


