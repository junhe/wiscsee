import argparse
import random

from sqlitedb import *


VALUE_SIZE = 100 # 100 is the default value size in leveldb
KEY_SIZE = 16 # 16 is the default key size in leveldb
# commit_period = 10


def parse_args():
    parser = argparse.ArgumentParser(
            description="Example: python bench.py -f /tmp/tmp.db -n 100 -p random")
    parser.add_argument('-f', '--dbpath', action='store', required=True)
    parser.add_argument('-n', '--ninsertions', action='store', required=True)
    parser.add_argument('-p', '--pattern', action='store', required=True)
    parser.add_argument('-e', '--period', action='store', required=True)
    args = parser.parse_args()

    return args


def run(db_path, n_insertions, pattern, commit_period):
    db = SqliteDB(db_path)
    db.open()
    db.initialize()

    if pattern == 'sequential':
        insert_sequentially(db, n_insertions, commit_period)
    elif pattern == 'random':
        insert_randomly(db, n_insertions, commit_period)
    elif pattern == 'preload_and_random':
        preload_and_randomly_insert(db, n_insertions, commit_period)
    else:
        raise NotImplementedError()

    db.commit()

    db.close()


def insert_sequentially(db, n_insertions, commit_period):
    print 'insert sequentially'
    for i in range(n_insertions):
        db.insert(key=encode_key(i), value='v' * VALUE_SIZE)
        if i % commit_period == 0 and i > 0:
            db.commit()


def insert_randomly(db, n_insertions, commit_period):
    print 'insert randomly'
    keys = range(n_insertions)
    random.shuffle(keys)
    for i, k in enumerate(keys):
        db.insert(key=encode_key(k), value='v' * VALUE_SIZE)
        if i % commit_period == 0 and i > 0:
            db.commit()


def preload_and_randomly_insert(db, n_insertions, commit_period):
    # preload
    print 'preload_and_randomly_insert'
    for i in range(n_insertions):
        db.insert(key=encode_key(i), value='v' * VALUE_SIZE)
    db.commit()
    print 'Preloaded', n_insertions, 'keys'

    update_randomly(db, n_insertions, commit_period)


def update_randomly(db, n_insertions, commit_period):
    print 'update randomly'
    keys = range(n_insertions)
    random.shuffle(keys)
    for i, k in enumerate(keys):
        db.update(key=encode_key(k), value='x' * VALUE_SIZE)
        if i % commit_period == 0 and i > 0:
            db.commit()

def encode_key(key):
    return str(key).zfill(KEY_SIZE)


def main():
    args = parse_args()

    run(args.dbpath, int(args.ninsertions), args.pattern, int(args.period))

if __name__ == '__main__':
    main()





