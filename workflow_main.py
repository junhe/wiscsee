#!/usr/bin/env python
import argparse

import FtlSim
import WlRunner

def main(args):
    # result dir
    dirpath = args.dir

    WlRunner.conf.config['result_dir'] = dirpath
    event_line_iter = WlRunner.main.run()

    confdic = FtlSim.utils.load_json('./FtlSim/config.json')
    confdic['output_dir'] = dirpath
    conf = FtlSim.config.Config(confdic)
    sim = FtlSim.simulator.Simulator(conf)
    sim.run(event_line_iter)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', action='store',
        help='the dir path for storing results (REQUIRED)')
    args = parser.parse_args()

    if args.dir == None:
        parser.print_help()
        exit(1)

    main(args)

