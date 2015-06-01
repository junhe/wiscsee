#!/usr/bin/env python

import ftl
from common import byte_to_pagenum

# TODO
# 1. read LBA layer trace
# 2. implement dmftl in ftl module
# 3. implement recorder.py

def read_lba_events(fpath):
    with open(fpath, 'r') as f:
        events = f.readlines()
    keys = ['operation', 'offset', 'size']
    for i, event in enumerate(events):
        event = event.strip('\n').split()
        event[1] = int(event[1]) # offset
        event[2] = int(event[2]) # size
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

def main():
    input_events = read_lba_events('./misc/lbaevents.sample')
    # input_events = read_lba_events('./misc/lbawrite.sample')

    for event in input_events:
        process_event(event)
        ftl.debug_after_processing()


if __name__ == '__main__':
    main()

