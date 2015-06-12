import argparse

import FtlSim
import WlRunner

def set_result_dir(dirpath):
    "set both module's result dir to dirpath"
    FtlSim.config.output_dir = dirpath
    WlRunner.conf.config['result_dir'] = dirpath

def main():
    event_line_iter = WlRunner.main.run()
    FtlSim.simmain.sim_run(event_line_iter)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', action='store',
        help='the dir path for storing results')
    args = parser.parse_args()

    if args.dir == None:
        parser.print_help()
        exit(1)

    set_result_dir(args.dir)

    main()

