

def get_job_template():
    """
    Since FIO job file is order sensitive, we use list to store
    the sections. Each section must have a 'section-name' key.
    """
    d = [ {
            'section-name': 'global',

            'ioengine':'libaio',
            'size':'1gb',
            'filename':'/dev/sdc',
            'direct':1,
            'bs':'64kb'
          },

          {
            'section-name': 'single-seq-read',
            'rw': 'read'
           }
        ]

    return d


def job_dict_to_string(job_dict):
    print job_dict
    table = []
    for section in job_dict:
        line = "[{}]".format(section['section-name'])
        table.append(line)
        for k, v in section.items():
            if k == 'section-name':
                continue
            sep = '='
            if v == None:
                sep = ''
                v = ''
            line = '{}{}{}'.format(k, sep, v)
            table.append(line)
        table.append('') # empty line

    # print table
    return '\n'.join(table)


def main():
    d = get_job_template()
    table = job_dict_to_string(d)
    print table

if __name__ == '__main__':
    main()


