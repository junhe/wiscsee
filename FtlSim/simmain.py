#!/usr/bin/env python

import argparse
from common import byte_to_pagenum, off_size_to_page_list
import sys

import ftl

# TODO
# 1. read LBA layer trace
# 2. implement dmftl in ftl module
# 3. implement recorder.py


# finish the testing chain
# - LBA pattern workload generator
# - seq, random, stride
# - pattern visualizor
# - LBA
# - flash page

# do sanity check with the above

# - block mapping
# - hybrid mapping

def read_lba_events(fpath):
    with open(fpath, 'r') as f:
        events = f.readlines()
    keys = ['operation', 'offset', 'size']
    for i, event in enumerate(events):
        event = event.strip('\n').split()
        event[1] = eval(event[1]) # offset
        # for debug
        # event[1] = eval(event[1])%(4096*4*16) #
        event[2] = eval(event[2]) # size
        events[i] = event

    events = [dict(zip(keys, e)) for e in events]
    return events

def process_event(event):
    pages = off_size_to_page_list(event['offset'], event['size'])

    if event['operation'] == 'read':
        for page in pages:
            ftl.lba_read(page)
    elif event['operation'] == 'write':
        for page in pages:
            ftl.lba_write(page)
    elif event['operation'] == 'discard':
        for page in pages:
            ftl.lba_discard(page)

def sim_run(eventfile):
    input_events = read_lba_events(eventfile)

    cnt = 0
    for event in input_events:
        process_event(event)
        ftl.debug_after_processing()
        cnt += 1
        if cnt % 10 == 0:
            print 'currnt count', cnt
            sys.stdout.flush()

def main():
    parser = argparse.ArgumentParser(
            description="It takes event input file."
            )
    parser.add_argument('-e', '--events', action='store', help='event file')
    args = parser.parse_args()

    if args.events == None:
        parser.print_help()
        exit(1)

    sim_run(args.events)

if __name__ == '__main__':
    main()

