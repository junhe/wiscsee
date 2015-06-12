import FtlSim
import WlRunner

def main():
    event_line_iter = WlRunner.main.run()
    FtlSim.simmain.sim_run(event_line_iter)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', action='store',
        help='the dir path for storing results')
    args = parser.parse_args()

    # if args.events == None:
        # parser.print_help()
        # exit(1)

    main()

