#!/usr/bin/env python

import ftl
import argparse
import recorder
from common import byte_to_pagenum

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
    if event['operation'] == 'read':
        ftl.lba_read(byte_to_pagenum(event['offset']))
    elif event['operation'] == 'write':
        ftl.lba_write(byte_to_pagenum(event['offset']))
    elif event['operation'] == 'discard':
        ftl.lba_discard(byte_to_pagenum(event['offset']))

def sim_run(eventfile):
    input_events = read_lba_events(eventfile)

    for event in input_events:
        process_event(event)
        ftl.debug_after_processing()

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

