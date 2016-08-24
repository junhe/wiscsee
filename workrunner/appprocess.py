import subprocess
from utilities import utils

class LevelDBProc(object):
    def __init__(self, benchmarks, num, db, outputpath,
            threads, use_existing_db, max_key, max_log):
        self.benchmarks = benchmarks
        self.num = num
        self.db = db
        self.outputpath = outputpath
        self.threads = threads
        self.use_existing_db = use_existing_db
        self.max_key = max_key
        self.max_log = max_log

    def run(self):
        utils.prepare_dir(self.db)

        db_bench_path = "../leveldb/db_bench"
        cmd = "{exe} --benchmarks={benchmarks} --num={num} --db={db} "\
                "--threads={threads}  "\
                "--dowrite_max_key={max_key} "\
                "--dowrite_skew_max_log={max_log} "\
                "--use_existing_db={use_existing_db} > {out}"\
            .format(
                exe = db_bench_path,
                benchmarks = self.benchmarks,
                num = self.num,
                db = self.db,
                out = self.outputpath,
                threads = self.threads,
                max_key = self.max_key,
                max_log = self.max_log,
                use_existing_db = self.use_existing_db
                )
        print cmd
        self.p = subprocess.Popen(cmd, shell=True)
        return self.p

    def wait(self):
        self.p.wait()


