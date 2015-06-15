#!/usr/bin/env python

import argparse
import sys

import dmftl
import config


def event_line_to_dic(line):
    keys = ['operation', 'offset', 'size']
    items = line.strip('\n').split()
    items[1] = eval(items[1]) # offset
    items[2] = eval(items[2]) # size
    event = dict(zip(keys, items))
    return event

def process_event(ftl, conf, event):
    """
    ftl should be a subclass of FtlBuilder
    TODO: check?
    """
    pages = conf.off_size_to_page_list(event['offset'], event['size'])

    if event['operation'] == 'read':
        for page in pages:
            ftl.lba_read(page)
    elif event['operation'] == 'write':
        for page in pages:
            ftl.lba_write(page)
    elif event['operation'] == 'discard':
        for page in pages:
            ftl.lba_discard(page)

def run(event_line_iter, confdic):
    """
    This is an interface for calling FTLSIM as a module. In order to call,
    you need provide an iterator of events and a dictionary containing
    configuration.

    It takes config as dict, not file path, because we later need to use
    FtlSim as module, and change config

    Note that confdic is a dictionary, not config.Config
    """

    # This should be the only place that we load config
    config.conf.load_from_dict(confdic)

    # you have to load configuration first before initialize recorder
    # recorder.initialize()

    ftl = dmftl.DmFtl(config.conf, None)

    for event_line in event_line_iter:
        event = event_line_to_dic(event_line)
        process_event(ftl, config.conf, event)

