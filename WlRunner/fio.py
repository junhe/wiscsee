import utils

NOVALUE, HIDE_ATTR = 'NOVALUE', 'HIDE_ATTR'

class JobDescription(dict):
    def __init__(self):
        """
        job_dict = { 'section3': {
                        'attribute1'=value1
                        ...
                      },
                   }
        section_order = ['section1', 'section2',..  ]

        ONLY use add_section() and remove_section() to add and remove sections.
        You can modify individual section directly by self.job_dict.

        """
        self.job_dict = {}
        self.section_order = []

    def add_section(self, section_dict):
        """
        Allow only one section in section_dict
        Must use this function to add section so we can keep the order
        """
        self.job_dict.update(section_dict)
        self.section_order.append(section_dict.keys()[0])

    def remove_section(self, section_name):
        del self.job_dict[section_name]
        self.section_order.remove(section_name)

    def __str__(self):
        table = []
        for section_name in self.section_order:
            line = "[{}]".format(section_name)
            table.append(line)

            for attr_name, attr_value in self.job_dict[section_name].items():
                sep = '='
                if attr_value == NOVALUE:
                    sep = ''
                    attr_value = ''
                elif attr_value == HIDE_ATTR:
                    # omit this line
                    continue
                line = '{}{}{}'.format(attr_name, sep, attr_value)
                table.append(line)
            table.append('') # empty line

        return '\n'.join(table)

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

def get_job_template():
    """
    Since FIO job file is order sensitive, we use list to store
    the sections. Each section must have a 'section-name' key.
    """
    d = [   {
            'global': {
                'ioengine':'libaio',
                'size':'1gb',
                'filename':'/dev/sdc',
                'direct':1,
                'bs':'64kb'
                }
            },

            {
            'single-seq-read': {
                'rw': 'read'
                }
            }
        ]

    return d

def main():
    job = JobDescription()
    job.add_section(
            {
                'global': {
                    'ioengine'   :'libaio',
                    'size'       :'1gb',
                    'filename'   :'/dev/sdc',
                    'direct'     :1,
                    'bs'         :'64kb'
                    }
            })
    print str(job)
    job.save('Iamhere')

if __name__ == '__main__':
    main()


