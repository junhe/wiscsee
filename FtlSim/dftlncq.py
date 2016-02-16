import bitarray
from collections import deque, Counter
import csv
import datetime
import random
import os
import Queue
import sys
import simpy

import bidict

import config
from commons import *
import flash
import flashcontroller
import ftlbuilder
import lrulist
import recorder
import utils
import dftldes


class NCQSingleQueue(object):
    """
    User of the queue can take up to depth # of request without
    returning
    """
    def __init__(self, ncq_depth, simpy_env):
        self.ncq_depth = ncq_depth
        self.env = simpy_env
        self.queue = simpy.Store(self.env)


class FTLwDFTL(object):
    """
    The interface of this FTL for the host is a queue (NCQ). The host puts
    requests to the queue with certain time intervals according to the
    blktrace.
    """
    def __init__(self, confobj, recorderobj, simpy_env):
        self.conf = confobj
        self.recorder = recorderobj
        self.env = simpy_env

        self.ncq = NCQSingleQueue(
                ncq_depth = self.conf['dftlncq']['ncq_depth'],
                simpy_env = self.env)

        self.flash_controller = flashcontroller.controller.Controller2(
                self.env, self.conf, self.recorder)

        self.realftl = dftldes.Dftl(self.conf, self.recorder,
                self.flash_controller, self.env)

        self.realftl.recorder.enable()

    def get_direct_mapped_flash_requests(self, io_req):
        """
        For example, read LPN 20 will executed as reading PPN 20
        """
        page_start, page_count = self.conf.sec_ext_to_page_ext(io_req.sector,
                io_req.sector_count)
        if io_req.operation == 'discard':
            return []
        elif io_req.operation in ('read', 'write'):
            return self.flash_controller.get_flash_requests_for_ppns(
                    page_start, page_count, op = io_req.operation)
        elif io_req.operation in ('enable_recorder', 'disable_recorder'):
            return []
        else:
            raise RuntimeError("operation {} is not supported".format(
                io_req.operation))

    def access_flash(self, flash_reqs):
        """
        This simpy process spawn multiple processes to access flash
        Some processes may be stalled if channel is busy
        """
        ctrl_procs = []
        for flash_req in flash_reqs:
            # This will have the requests automatically queued at channel
            p = self.env.process(
                    self.flash_controller.execute_request(flash_req))
            ctrl_procs.append(p)

        all_ctrl_procs = simpy.events.AllOf(self.env, ctrl_procs)
        yield all_ctrl_procs

    def process(self, pid):
        req_index = 0
        while True:
            io_req = yield self.ncq.queue.get()
            # print "At time {} [{}] got request ({}) {}".format(self.env.now,
                    # pid, req_index, str(io_req))

            if io_req.operation == 'end_process':
                break

            s = self.env.now
            flash_reqs = yield self.env.process(
                    self.realftl.translate(io_req, pid) )
            e = self.env.now
            # print "Translation took", e - s
            self.recorder.add_to_timer("translation_time-w_wait", pid,
                    e - s)

            s = self.env.now
            yield self.env.process(
                    self.access_flash(flash_reqs))
            # print "At time {} [{}] finish request ({})".format(self.env.now,
                    # pid, req_index)
            e = self.env.now
            self.recorder.add_to_timer("forground_flash_access_time-w_wait", pid,
                    e - s)

            # Try clean garbage
            self.env.process(
                    self.realftl.clean_garbage())

            req_index += 1

            if pid == 0 and req_index % 100 == 0:
                print self.env.now / float(SEC)

    def run(self):
        procs = []
        for i in range(self.conf['dftlncq']['ncq_depth']):
            p = self.env.process( self.process(i) )
            procs.append(p)
        e = simpy.events.AllOf(self.env, procs)
        yield e
        print "++++++++++++++++++++++++END OF FTL+++++++++++++++++++++++++++"
        print "Time:", float(self.env.now) / SEC


