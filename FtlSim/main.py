#!/usr/bin/env python

import ftl

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
        ftl.lba_read(event['offset'], event['size'])
    elif event['operation'] == 'write':
        ftl.lba_write(event['offset'], event['size'])
    elif event['operation'] == 'discard':
        ftl.lba_discard(event['offset'], event['size'])

def main():
    input_events = read_lba_events('./misc/lbaevents.sample')

    for event in input_events:
        process_event(event)

if __name__ == '__main__':
    main()

