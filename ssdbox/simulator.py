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
import hostevent
import dftldes

from commons import *
from ftlsim_commons import *

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
        self.recorder = recorder.Recorder(output_target = self.conf['output_target'],
            output_directory = self.conf['result_dir'],
            verbose_level = self.conf['verbose_level'],
            print_when_finished = self.conf['print_when_finished']
            )

        if self.conf.has_key('enable_e2e_test'):
            raise RuntimeError("enable_e2e_test is deprecated")


def random_data(addr):
    randnum = random.randint(0, 10000)
    content = "{}.{}".format(addr, randnum)
    return content


class Host(object):
    def __init__(self, conf, simpy_env, event_iter):
        self.conf = conf
        self.env = simpy_env
        self.event_iter = event_iter

        self._ncq = NCQSingleQueue(
                ncq_depth = self.conf['SSDFramework']['ncq_depth'],
                simpy_env = self.env)

    def get_ncq(self):
        return self._ncq

    def _process(self):
        for event in self.event_iter:
            yield self._ncq.queue.put(event)

    def run(self):
        yield self.env.process(self._process())
        yield self._ncq.queue.put(hostevent.ControlEvent("shut_ssd"))


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

        self.ftl = ftl_class(self.conf, self.recorder,
            flash.Flash(recorder = self.recorder, confobj = self.conf))

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
        if event.operation == OP_READ:
            self.read(event)
        elif event.operation == OP_WRITE:
            self.write(event)
        elif event.operation == OP_DISCARD:
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
        self.ssdframework = ssdframework.SSDFramework(self.conf, self.recorder, self.env)

    def host_proc(self):
        """
        This process acts like a producer, putting requests to ncq
        """

        # this token is acquired before we issue request to queue.
        # it effectively control the queue depth of this process
        token = simpy.Resource(self.env,
                capacity = self.conf['process_queue_depth'])

        for event in self.event_iter:
            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

        for i in range(self.conf['SSDFramework']['ncq_depth']):
            event = hostevent.ControlEvent("shut_ssd")

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


class SimulatorDESNew(Simulator):
    def __init__(self, conf, event_iter):
        super(SimulatorDESNew, self).__init__(conf, event_iter)

        self.env = simpy.Environment()
        self.host = Host(self.conf, self.env, event_iter)
        self.ssd = ssdframework.Ssd(self.conf, self.env,
                self.host.get_ncq(), self.recorder)

    def run(self):
        self.env.process(self.host.run())
        self.env.process(self.ssd.run())

        self.env.run()

    def get_sim_type(self):
        return "SimulatorDESNew"


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
        self.ssdframework = ssdframework.SSDFramework(self.conf, self.recorder, self.env)

    def host_proc(self, pid, event_iter):
        """
        This process acts like a producer, putting requests to ncq
        """

        # this token is acquired before we issue request to queue.
        # it effectively control the queue depth of this process
        token = simpy.Resource(self.env,
                capacity = self.conf['process_queue_depth'])

        for event in event_iter:
            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

        for i in range(self.conf['SSDFramework']['ncq_depth']):
            event = hostevent.ControlEvent("shut_ssd")

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

class SimulatorDESTime(Simulator):
    def __init__(self, conf, event_iter):
        super(SimulatorDESTime, self).__init__(conf, None)


        self.event_iter = event_iter

        self.env = simpy.Environment()
        self.ssdframework = ssdframework.SSDFramework(self.conf, self.recorder,
                self.env)

    def host_proc(self, event_iter):
        """
        This process acts like a producer, putting requests to ncq
        """

        # this token is acquired before we issue request to queue.
        # it effectively control the queue depth of this process
        token = simpy.Resource(self.env,
                capacity = self.conf['process_queue_depth'])

        for event in event_iter:
            event.token = token
            event.token_req = event.token.request()

            if self.conf['simulator_enable_interval'] == True:
                if event.operation in (OP_READ, OP_WRITE, OP_DISCARD):
                    yield self.env.timeout(int(event.pre_wait_time * SEC))

            yield self.ssdframework.ncq.queue.put(event)

        for i in range(self.conf['SSDFramework']['ncq_depth']):
            event = hostevent.ControlEvent("shut_ssd")

            event.token = token
            event.token_req = event.token.request()

            yield event.token_req

            yield self.ssdframework.ncq.queue.put(event)

        print 'Host finish time', self.env.now

    def run(self):
        self.env.process(self.host_proc(self.event_iter))
        self.env.process(self.ssdframework.run())

        self.env.run()

    def get_sim_type(self):
        return "SimulatorDESTime"

    def write(self):
        raise NotImplementedError()

    def read(self):
        raise NotImplementedError()

    def discard(self):
        raise NotImplementedError()


