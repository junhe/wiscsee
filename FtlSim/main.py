# for executing FtlSim from command line
import argparse

import utils
import config
import recorder
import simulator

def main():
    parser = argparse.ArgumentParser(
            description="It takes event input file."
            )
    parser.add_argument('-c', '--configfile', action='store',
        help='config file path (REQUIRED)')
    parser.add_argument('-e', '--events', action='store',
        help='event file (REQUIRED)')
    parser.add_argument('-v', '--verbose', action='store',
        help='verbose level: 0-minimum, 1-more')
    args = parser.parse_args()

    if args.events == None:
        parser.print_help()
        exit(1)

    if args.configfile == None:
        parser.print_help()
        exit(1)


    # Go with the wind~~~~
    conf = config.Config()
    conf.load_from_json_file(args.configfile)

    if args.verbose != None:
        conf['verbose_level'] = int(args.verbose)

    sim = simulator.Simulator(conf)
    sim.run(open(args.events, 'r'))

if __name__ == '__main__':
    main()

