#!/usr/bin/env python
import argparse

import FtlSim
import WlRunner

WLRUNNER, LBAGENERATOR = ('WLRUNNER', 'LBAGENERATOR')

def main(args):
    # result dir
    dirpath = args.dir

    # run the workload
    runner_conf = WlRunner.config.Config()
    runner_conf.load_from_json_file('config.json')
    runner_conf['result_dir'] = dirpath

    workload_src = LBAGENERATOR
    if workload_src == WLRUNNER:
        runner = WlRunner.wlrunner.WorkloadRunner(runner_conf)
        event_iter = runner.run()
    elif workload_src == LBAGENERATOR:
        lbagen = WlRunner.lbaworkloadgenerator.Sequential(
            runner_conf['flash_page_size'],
            runner_conf['flash_npage_per_block'],
            runner_conf['flash_num_blocks'])
        event_iter = lbagen

    # run the Ftl Simulator
    confdic = FtlSim.utils.load_json('config.json')
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

