import subprocess
import pprint

KB = 2**10
MB = 2**20
GB = 2**30

translator = {  'file_size': '-f',
                'write_size': '-w',
                'n_writes': '-n',
                'pattern': '-p',
                'fsync': '-y',
                'sync': '-s',
                'file_path': '-l',
                'tag': '-t',
                'markerfile': '-m'
              }


def parse_player_runtime_out(lines):
    d = {}
    for line in lines:
        items = line.split()
        if len(items) == 2:
            d[items[0]] = items[1]

    return d


class MultiWriters(object):
    def __init__(self, player_path, parameters):
        """
        parameters is a list of dictionaries
        [
          { 'file_size':
            'write_size':
            'n_writes':
            'pattern':
            'fsync':
            'sync':
            'file_path':
            'tag':
            'markerfile':
          },
          ...
        ]
        """
        args = []
        for para in parameters:
            arg = [player_path, ]
            for k, v in para.items():
                arg.append(translator[k])
                arg.append(str(v))
            args.append(arg)

        # each row is a args for a player instance
        self.args_table = args

    def run(self):
        procs = []

        for args in self.args_table:
            print ' '.join(args)
            p = subprocess.Popen(args, stdout = subprocess.PIPE)
            procs.append(p)

        for p in procs:
            p.wait()

        results = []
        for p in procs:
            if p.returncode != 0:
                raise RuntimeError("multiwriter process fails. PID={}".format(
                    p.pid))

            lines = p.communicate()[0].split('\n')
            d = parse_player_runtime_out(lines)
            d['pid.python'] = p.pid
            results.append(d)

        # pprint.pprint( results )
        return results


def main():
    parameters = [
          { 'file_size': 256 * MB,
            'write_size': 64 * KB,
            'n_writes': 4 * 256 * MB / (64 * KB),
            'pattern': 'random',
            'fsync': 1,
            'sync': 0,
            'file_path': '/mnt/fsonloop/file01',
            'tag': 'mytag001'
          },
          { 'file_size': 256 * MB,
            'write_size': 64 * KB,
            'n_writes': 4 * 256 * MB / (64 * KB),
            'pattern': 'random',
            'fsync': 1,
            'sync': 0,
            'file_path': '/mnt/fsonloop/file01',
            'tag': 'mytag002'
          }
    ]

    mw = MultiWriters('./player-runtime', parameters)
    pprint.pprint( mw.run() )


if __name__ == '__main__':
    main()

