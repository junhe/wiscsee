#!/usr/bin/env python
import argparse

import FtlSim
import WlRunner

WLRUNNER, LBAGENERATOR = ('WLRUNNER', 'LBAGENERATOR')

def main(args):
    # result dir
    dirpath = args.dir

    # load configs
    runner_conf = WlRunner.config.Config()
    runner_conf.load_from_json_file('config.wlrunner.json')
    runner_conf['result_dir'] = dirpath

    ftlsim_conf = FtlSim.config.Config()
    ftlsim_conf.load_from_json_file('./config.ftlsim.json')
    ftlsim_conf['output_dir'] = dirpath

    # run the workload
    workload_src = LBAGENERATOR
    if workload_src == WLRUNNER:
        runner = WlRunner.wlrunner.WorkloadRunner(runner_conf)
        event_iter = runner.run()
    elif workload_src == LBAGENERATOR:
        lbagen = eval("""WlRunner.lbaworkloadgenerator.{classname}(
            ftlsim_conf['flash_page_size'],
            ftlsim_conf['flash_npage_per_block'],
            ftlsim_conf['flash_num_blocks'])""".format(
            classname=runner_conf['lba_workload_class']))
        event_iter = lbagen

    # run the Ftl Simulator
    sim = FtlSim.simulator.Simulator(ftlsim_conf)
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

