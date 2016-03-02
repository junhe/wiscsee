#!/usr/bin/env python
import abc
import argparse
import random
import simpy
import sys

import bmftl
import config
import ssdframework
import dftlext
import dmftl
import flash
import nkftl2
import recorder

from commons import *


class EventSimple(object):
    def __init__(self, pid, operation):
        self.pid = int(pid)
        self.operation = operation

    def __str__(self):
        return "EventSimple PID:{} OP:{}".format(self.pid, self.operation)


class Event(object):
    def __init__(self, sector_size, pid, operation, offset, size, sync = True):
        self.pid = int(pid)
        self.operation = operation
        self.offset = int(offset)
        self.size = int(size)
        self.sync = sync

        assert self.offset % sector_size == 0,\
            "offset {} is not aligned with sector size {}.".format(
            self.offset, sector_size)
        self.sector = self.offset / sector_size

        assert self.size % sector_size == 0, \
            "size {} is not multiple of sector size {}".format(
            self.size, sector_size)

        self.sector_count = self.size / sector_size

    def __str__(self):
        return "Event pid:{pid}, operation:{operation}, offset:{offset}, "\
                "size:{size}, sector:{sector}, sector_count:{sector_count}, "\
                "sync:{sync}"\
                .format(pid = self.pid, operation = self.operation,
                        offset = self.offset, size = self.size,
                        sector = self.sector, sector_count = self.sector_count,
                        sync = self.sync)

class Event2(Event):
    """
    Add sync as attribute. It is a boolean.
    """
    def __init__(self, sector_size, pid, operation, offset, size, sync):
        super(Event2, self).__init__(sector_size, pid, operation, offset, size)
        self.sync = sync

    def __str__(self):
        return super(Event2, self).__str__() + ", sync: {}".format(self.sync)


class Simulator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run(self):
        return

    @abc.abstractmethod
    def get_sim_type(self):
        return

    @abc.abstractmethod
    def write(self):
        return

    @abc.abstractmethod
    def read(self):
        return

    @abc.abstractmethod
    def discard(self):
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

        if self.conf.has_key('enable_e2e_test'):
            raise RuntimeError("enable_e2e_test is deprecated")


def random_data(addr):
    randnum = random.randint(0, 10000)
    content = "{}.{}".format(addr, randnum)
    return content


class SimulatorNonDES(Simulator):
    __metaclass__ = abc.ABCMeta

    def __init__(self, conf, event_iter):
        super(SimulatorNonDES, self).__init__(conf, event_iter)

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
        elif self.conf['ftl_type'] == 'dftlext':
            ftl_class = dftlext.Dftl
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

    def run(self):
        """
        You must garantee that each item in event_iter is a class Event
        """
        cnt = 0
        for event in self.event_iter:
            self.process_event(event)
            cnt += 1
            if cnt % 100 == 0:
                print '|',
                sys.stdout.flush()

        self.ftl.post_processing()

    def process_event(self, event):
        if event.operation == 'read':
            self.read(event)
        elif event.operation == 'write':
            self.write(event)
        elif event.operation == 'discard':
            self.discard(event)
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


class SimulatorNonDESSpeed(SimulatorNonDES):
    """
    This one does not do e2e test
    It uses extents
    """
    def get_sim_type(self):
        return "NonDESSpeed"

    def write(self, event):
        self.ftl.sec_write(
                sector = event.sector,
                count = event.sector_count,
                data = None)

    def read(self, event):
        """
        read extent from flash and check if the data is correct.
        """
        self.ftl.sec_read(
                sector = event.sector,
                count = event.sector_count)

    def discard(self, event):
        self.ftl.sec_discard(
            sector = event.sector,
            count = event.sector_count)


class SimulatorNonDESe2e(SimulatorNonDES):
    """
    This one does not do e2e test
    It uses extents
    """
    def __init__(self, conf, event_iter):
        super(SimulatorNonDESe2e, self).__init__(conf, event_iter)

        self.lsn_to_data = {}

    def get_sim_type(self):
        return "NonDESe2e"

    def write(self, event):
        """
        1. Generate random data
        2. Copy random data to lsn_to_data
        3. Write data by ftl
        """
        data = []
        for sec in range(event.sector, event.sector + event.sector_count):
            content = random_data(sec)
            self.lsn_to_data[sec] = content
            data.append(content)

        self.ftl.sec_write(
                sector = event.sector,
                count = event.sector_count,
                data = data)

    def read(self, event):
        """
        read extent from flash and check if the data is correct.
        """
        data = self.ftl.sec_read(
                sector = event.sector,
                count = event.sector_count)

        self.check_read(event, data)

    def check_read(self, event, data):
        for sec, sec_data in zip(
                range(event.sector, event.sector + event.sector_count), data):
            if self.lsn_to_data.get(sec, None) != sec_data:
                msg = "Data is not correct. Got: {read}, "\
                        "Correct: {correct}. sector={sec}".format(
                        read = sec_data,
                        correct = self.lsn_to_data.get(sec, None),
                        sec = sec)
                print msg
                # raise RuntimeError(msg)

    def discard(self, event):
        self.ftl.sec_discard(
            sector = event.sector,
            count = event.sector_count)

        for sec in range(event.sector, event.sector + event.sector_count):
            try:
                del self.lsn_to_data[sec]
            except KeyError:
                pass


class SimulatorNonDESe2elba(SimulatorNonDES):
    """
    This one is e2e and use lba interface
    """
    def __init__(self, conf, event_iter):
        super(SimulatorNonDESe2elba, self).__init__(conf, event_iter)

        self.lpn_to_data = {}

    def get_sim_type(self):
        return "NonDESe2elba"

    def write(self, event):
        pages = self.conf.off_size_to_page_list(event.offset,
            event.size)
        for page in pages:
            content = random_data(page)
            self.ftl.lba_write(page, data = content, pid = event.pid)
            self.lpn_to_data[page] = content

    def read(self, event):
        pages = self.conf.off_size_to_page_list(event.offset,
            event.size, force_alignment = False)
        for page in pages:
            data = self.ftl.lba_read(page, pid = event.pid)
            correct = self.lpn_to_data.get(page, None)
            if data != correct:
                print "!!!!!!!!!! Correct: {}, Got: {}".format(
                self.lpn_to_data.get(page, None), data)
                raise RuntimeError()

    def discard(self, event):
        pages = self.conf.off_size_to_page_list(event.offset,
            event.size)
        for page in pages:
            self.ftl.lba_discard(page, pid = event.pid)
            try:
                del self.lpn_to_data[page]
            except KeyError:
                pass


class SimulatorDES(Simulator):
    def __init__(self, conf, event_iter):
        super(SimulatorDES, self).__init__(conf, event_iter)

        self.env = simpy.Environment()
        self.ssdframework = ssdframework.SSDFramework(self.conf, self.rec, self.env)

    def host_proc(self):
        """
        This process acts like a producer, putting requests to ncq
        """

        # this token is acquired before we issue request to queue.
        # it effectively control the queue depth of this process
        token = simpy.Resource(self.env, capacity = 1)

        for event in self.event_iter:
            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

        for i in range(self.conf['SSDFramework']['ncq_depth']):
            event = EventSimple(0, "end_process")

            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

    def run(self):
        self.env.process(self.host_proc())
        self.env.process(self.ssdframework.run())

        self.env.run()

    def get_sim_type(self):
        return "SimulatorDES"

    def write(self):
        raise NotImplementedError()

    def read(self):
        raise NotImplementedError()

    def discard(self):
        raise NotImplementedError()

class SimulatorDESSync(Simulator):
    def __init__(self, conf, event_iters):
        """
        event_iters is list of event iterators
        """
        super(SimulatorDESSync, self).__init__(conf, None)

        if not isinstance(event_iters, list):
            raise RuntimeError("event_iters must be a list of iterators.")

        self.event_iters = event_iters

        self.env = simpy.Environment()
        self.ssdframework = ssdframework.SSDFramework(self.conf, self.rec, self.env)

    def host_proc(self, pid, event_iter):
        """
        This process acts like a producer, putting requests to ncq
        """

        # this token is acquired before we issue request to queue.
        # it effectively control the queue depth of this process
        token = simpy.Resource(self.env, capacity = 1)

        for event in event_iter:
            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

        for i in range(self.conf['SSDFramework']['ncq_depth']):
            event = EventSimple(0, "end_process")

            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

    def run(self):
        for i, event_iter in enumerate(self.event_iters):
            self.env.process(self.host_proc(i, event_iter))
        self.env.process(self.ssdframework.run())

        self.env.run()

    def get_sim_type(self):
        return "SimulatorDES"

    def write(self):
        raise NotImplementedError()

    def read(self):
        raise NotImplementedError()

    def discard(self):
        raise NotImplementedError()





