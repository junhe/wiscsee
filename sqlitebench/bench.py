import argparse
import random

from sqlitedb import *


VALUE_SIZE = 100 # 100 is the default value size in leveldb
KEY_SIZE = 16 # 16 is the default key size in leveldb
COMMIT_PEROID = 10


def parse_args():
    parser = argparse.ArgumentParser(
            description="Example: python bench.py -f /tmp/tmp.db -n 100 -p random")
    parser.add_argument('-f', '--dbpath', action='store', required=True)
    parser.add_argument('-n', '--ninsertions', action='store', required=True)
    parser.add_argument('-p', '--pattern', action='store', required=True)
    args = parser.parse_args()

    return args


def run(db_path, n_insertions, pattern):
    db = SqliteDB(db_path)
    db.open()
    db.initialize()

    if pattern == 'sequential':
        insert_sequentially(db, n_insertions)
    elif pattern == 'random':
        insert_randomly(db, n_insertions)
    elif pattern == 'preload_and_random':
        preload_and_randomly_insert(db, n_insertions)
    else:
        raise NotImplementedError()

    db.commit()

    db.close()


def insert_sequentially(db, n_insertions):
    for i in range(n_insertions):
        db.insert(key=encode_key(i), value='v' * VALUE_SIZE)
        if i % COMMIT_PEROID == 0 and i > 0:
            db.commit()


def insert_randomly(db, n_insertions):
    keys = range(n_insertions)
    random.shuffle(keys)
    for i, k in enumerate(keys):
        db.insert(key=encode_key(k), value='v' * VALUE_SIZE)
        if i % COMMIT_PEROID == 0 and i > 0:
            db.commit()


def preload_and_randomly_insert(db, n_insertions):
    # preload
    for i in range(n_insertions):
        db.insert(key=encode_key(i), value='v' * VALUE_SIZE)
    db.commit()

    update_randomly(db, n_insertions)


def update_randomly(db, n_insertions):
    keys = range(n_insertions)
    random.shuffle(keys)
    for i, k in enumerate(keys):
        db.update(key=encode_key(k), value='x' * VALUE_SIZE)
        if i % COMMIT_PEROID == 0 and i > 0:
            db.commit()

def encode_key(key):
    return str(key).zfill(KEY_SIZE)


def main():
    args = parse_args()

    run(args.dbpath, int(args.ninsertions), args.pattern)

if __name__ == '__main__':
    main()





