#!/usr/bin/env python
import argparse

import config
import FtlSim
import WlRunner

WLRUNNER, LBAGENERATOR = ('WLRUNNER', 'LBAGENERATOR')

def main(args):
    # result dir
    dirpath = args.dir

    # load configs
    conf = config.Config()
    conf.load_from_json_file('config.json')
    conf['result_dir'] = dirpath
    conf['output_dir'] = dirpath

    # run the workload
    workload_src = LBAGENERATOR
    if workload_src == WLRUNNER:
        runner = WlRunner.wlrunner.WorkloadRunner(conf)
        event_iter = runner.run()
    elif workload_src == LBAGENERATOR:
        lbagen = eval("""WlRunner.lbaworkloadgenerator.{classname}(
            conf['flash_page_size'],
            conf['flash_npage_per_block'],
            conf['flash_num_blocks'])""".format(
            classname=conf['lba_workload_class']))
        event_iter = lbagen

    # run the Ftl Simulator
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

