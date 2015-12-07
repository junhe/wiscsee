
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
                if attr_value == None:
                    sep = ''
                    attr_value = ''
                line = '{}{}{}'.format(attr_name, sep, attr_value)
                table.append(line)
            table.append('') # empty line

        return '\n'.join(table)

    def save(self, filepath):
        with open(filepath, 'w') as f:
            f.write(str(self))

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


