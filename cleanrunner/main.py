import config
import wlrunner

def main():
    confobj = config.Config()
    confobj.load_from_json_file('./config.json')
    runner = wlrunner.WorkloadRunner(confobj)
    it = runner.run()

if __name__ == '__main__':
    main()
