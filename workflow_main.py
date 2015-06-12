import FtlSim
import WlRunner

def main():
    event_line_iter = WlRunner.main.run()
    FtlSim.simmain.sim_run(event_line_iter)

if __name__ == '__main__':
    main()

