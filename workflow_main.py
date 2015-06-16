#!/usr/bin/env python
import argparse

import FtlSim
import WlRunner

def main(args):
    # result dir
    dirpath = args.dir

    runner_conf = WlRunner.config.Config()
    runner_conf.load_from_json_file('./WlRunner/config.json')
    runner_conf['result_dir'] = dirpath
    runner = WlRunner.wlrunner.WorkloadRunner(runner_conf)
    event_iter = runner.run()

    confdic = FtlSim.utils.load_json('./FtlSim/config.json')
    confdic['output_dir'] = dirpath
    conf = FtlSim.config.Config(confdic)
    sim = FtlSim.simulator.Simulator(conf)
    sim.run(event_iter)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', action='store',
        help='the dir path for storing results (REQUIRED)')
    args = parser.parse_args()

    if args.dir == None:
        parser.print_help()
        exit(1)

    main(args)

