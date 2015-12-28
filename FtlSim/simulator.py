#!/usr/bin/env python

import abc
import argparse
import random
import simpy
import sys

import bmftl
import config
import dftl2
import dftlDES
import dmftl
import flash
import hmftl
import nkftl
import nkftl2
import pmftl
import recorder
import tpftl

from commons import *


class Event(object):
    def __init__(self, pid, operation, offset, size):
        self.pid = pid
        self.operation = operation
        self.offset = offset
        self.size = size

# Convert before comming here
# def event_line_to_event(line):
    # keys = ['pid', 'operation', 'offset', 'size']
    # items = line.strip('\n').split()
    # # items[1] = eval(items[1]) # offset
    # # items[2] = eval(items[2]) # size
    # event = dict(zip(keys, items))
    # return event

class Simulator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run(self):
        return

    @abc.abstractmethod
    def get_sim_type(self):
        return

    def __init__(self, conf, event_iter):
        "conf is class Config"
        if not isinstance(conf, config.Config):
            raise TypeError("conf is not config.Config, it is {}".
                format(type(conf).__name__))

        self.conf = conf
        self.event_iter = event_iter

        # initialize recorder
        self.rec = recorder.Recorder(output_target = self.conf['output_target'],
            path = self.conf.get_output_file_path(),
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )

        if self.conf['ftl_type'] == 'directmap':
            ftl_class = dmftl.DirectMapFtl
        elif self.conf['ftl_type'] == 'blockmap':
            ftl_class = bmftl.BlockMapFtl
        elif self.conf['ftl_type'] == 'pagemap':
            ftl_class = pmftl.PageMapFtl
        elif self.conf['ftl_type'] == 'hybridmap':
            ftl_class = hmftl.HybridMapFtl
        elif self.conf['ftl_type'] == 'dftl2':
            ftl_class = dftl2.Dftl
        elif self.conf['ftl_type'] == 'dftlDES':
            ftl_class = dftlDES.Dftl
        elif self.conf['ftl_type'] == 'tpftl':
            ftl_class = tpftl.Tpftl
        elif self.conf['ftl_type'] == 'nkftl':
            raise DeprecationWarning("You are trying to use nkftl, which is a "
                "deprecated version of nkftl. Please use nkftl2 instead.")
            ftl_class = nkftl.Nkftl
        elif self.conf['ftl_type'] == 'nkftl2':
            ftl_class = nkftl2.Nkftl
        else:
            raise ValueError("ftl_type {} is not defined"\
                .format(self.conf['ftl_type']))

        self.ftl = ftl_class(self.conf, self.rec,
            flash.Flash(recorder = self.rec, confobj = self.conf))

        if self.conf['ftl_type'] == 'tpftl':
            self.interface_level = 'range'
        else:
            self.interface_level = 'page'

        if self.conf['enable_e2e_test'] == True:
            self.event_processor = self.process_event_e2e_test
            self.lpn_to_data = {}
        else:
            self.event_processor = self.process_event

    def process_event_e2e_test(self, event):
        if event.operation == 'read':
            event.offset = int(event.offset)
            event.size = int(event.size)
            assert self.interface_level == 'page'
            pages = self.conf.off_size_to_page_list(event.offset,
                event.size, force_alignment = False)
            for page in pages:
                data = self.ftl.lba_read(page, pid = event.pid)
                correct = self.lpn_to_data.get(page, None)
                if data != correct:
                    print "!!!!!!!!!! Correct: {}, Got: {}".format(
                    self.lpn_to_data.get(page, None), data)
                    raise RuntimeError()

        elif event.operation == 'write':
            event.offset = int(event.offset)
            event.size = int(event.size)
            assert self.interface_level == 'page'
            pages = self.conf.off_size_to_page_list(event.offset,
                event.size)
            for page in pages:
                # generate random content
                content = random.randint(0, 10000)
                content = "{}.{}".format(page, content)
                self.ftl.lba_write(page, data = content, pid = event.pid)
                self.lpn_to_data[page] = content

        elif event.operation == 'discard':
            event.offset = int(event.offset)
            event.size = int(event.size)
            assert self.interface_level == 'page'
            pages = self.conf.off_size_to_page_list(event.offset,
                event.size)
            for page in pages:
                self.ftl.lba_discard(page, pid = event.pid)
                try:
                    del self.lpn_to_data[page]
                except KeyError:
                    pass

        elif event.operation == 'enable_recorder':
            self.ftl.enable_recording()
        elif event.operation == 'disable_recorder':
            self.ftl.disable_recording()
        elif event.operation == 'workloadstart':
            self.ftl.pre_workload()
        elif event.operation == 'finish':
            # ignore this
            pass
        else:
            print event
            raise RuntimeError("operation '{}' is not supported".format(
                event.operation))

    def process_event(self, event):
        if event.operation == 'read':
            event.offset = int(event.offset)
            event.size = int(event.size)
            if self.interface_level == 'page':
                pages = self.conf.off_size_to_page_list(event.offset,
                    event.size, force_alignment = False)
                for page in pages:
                    self.ftl.lba_read(page, pid = event.pid)
            elif self.interface_level == 'range':
                start_page, npages = self.conf.off_size_to_page_range(
                    event.offset, event.size, force_alignment = False)
                self.ftl.read_range(start_page, npages)
            else:
                raise RuntimeError("interface_level {} not supported".format(
                    self.interface_level))

        elif event.operation == 'write':
            event.offset = int(event.offset)
            event.size = int(event.size)
            if self.interface_level == 'page':
                pages = self.conf.off_size_to_page_list(event.offset,
                    event.size)
                for page in pages:
                    self.ftl.lba_write(page, pid = event.pid)
            elif self.interface_level == 'range':
                start_page, npages = self.conf.off_size_to_page_range(
                    event.offset, event.size)
                self.ftl.write_range(start_page, npages)
            else:
                raise RuntimeError("interface_level {} not supported".format(
                    self.interface_level))

        elif event.operation == 'discard':
            event.offset = int(event.offset)
            event.size = int(event.size)
            if self.interface_level == 'page':
                pages = self.conf.off_size_to_page_list(event.offset,
                    event.size)
                for page in pages:
                    self.ftl.lba_discard(page, pid = event.pid)
            elif self.interface_level == 'range':
                start_page, npages = self.conf.off_size_to_page_range(
                    event.offset, event.size)
                self.ftl.discard_range(start_page, npages)
            else:
                raise RuntimeError("interface_level {} not supported".format(
                    self.interface_level))

        elif event.operation == 'enable_recorder':
            self.ftl.enable_recording()
        elif event.operation == 'disable_recorder':
            self.ftl.disable_recording()
        elif event.operation == 'workloadstart':
            self.ftl.pre_workload()
        elif event.operation == 'finish':
            # ignore this
            pass
        else:
            raise RuntimeError("operation '{}' is not supported".format(
                event.operation))

class SimulatorNonDES(Simulator):
    def get_sim_type(self):
        return "NonDES"

    def run(self):
        """
        You must garantee that each item in event_iter is a class Event
        """
        cnt = 0
        for event in self.event_iter:
            self.event_processor(event)
            cnt += 1
            if cnt % 100 == 0:
                print '|',
                sys.stdout.flush()

        self.ftl.post_processing()

class SimulatorDES(Simulator):
    def get_sim_type(self):
        return "DES"

    def sim_proc(self):
        cnt = 0
        for event in self.event_iter:
            self.event_processor(event)
            yield self.env.timeout(1 * MSEC)
            cnt += 1
            if cnt % 100 == 0:
                print '|',
                sys.stdout.flush()

        self.ftl.post_processing()

        print self.env.now

    def setup(self):
        self.env = simpy.Environment()
        self.env.process(self.sim_proc())

    def run(self):
        self.setup()
        self.env.run()
        print 'simulator type:', self.get_sim_type()


