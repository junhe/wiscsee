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


class FTL(object):
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

        self.flash_controller = flashcontroller.controller.Controller(
                self.env, self.conf)


    def get_direct_mapped_flash_requests(self, io_req):
        page_start, page_count = self.conf.sec_ext_to_page_ext(io_req.sector,
                io_req.sector_count)
        if io_req.operation == 'discard':
            return []
        elif io_req.operation in ('read', 'write'):
            return self.get_flash_requests_for_page(page_start, page_count,
                    op = io_req.operation)
        elif io_req.operation in ('enable_recorder', 'disable_recorder'):
            return []
        else:
            raise RuntimeError("operation {} is not supported".format(
                io_req.operation))

    def get_flash_requests_for_block(self, block_start, block_count, op):
        ret_requests = []
        for block in range(block_start, block_start + block_count):
            machine_block_addr = \
                    self.flash_controller.physical_to_machine_block(block)
            flash_req = flashcontroller.controller.create_flashrequest(
                    machine_block_addr, op = op)
            ret_requests.append(flash_req)

        return ret_requests

    def get_flash_requests_for_page(self, page_start, page_count, op):
        """
        op can be 'read', 'write', and 'erase'
        """
        ret_requests = []
        for page in range(page_start, page_start + page_count):
            machine_page_addr = \
                self.flash_controller.physical_to_machine_page(page)
            flash_req = flashcontroller.controller.create_flashrequest(
                    machine_page_addr, op = op)
            ret_requests.append(flash_req)

        return ret_requests

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
            # print pid, 'Got request (', req_index, io_req.operation, ') at time', self.env.now

            flash_reqs = self.get_direct_mapped_flash_requests(io_req)

            yield self.env.process(
                    self.access_flash(flash_reqs))
            # print pid, 'Finish request (', req_index, io_req.operation, ') at time', self.env.now

            req_index += 1

    def run(self):
        for i in range(self.conf['dftlncq']['ncq_depth']):
            self.env.process( self.process(i) )
        # no need to wait for all processes to finish here because
        # this function is not a simpy process


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
            print "At time {} [{}] got request ({}) {}".format(self.env.now,
                    pid, req_index, str(io_req))

            if io_req.operation == 'end_process':
                break

            s = self.env.now
            flash_reqs = yield self.env.process( self.realftl.translate(io_req) )
            e = self.env.now
            print "Translation took", e - s

            s = self.env.now
            yield self.env.process(
                    self.access_flash(flash_reqs))
            print "At time {} [{}] finish request ({})".format(self.env.now,
                    pid, req_index)
            e = self.env.now
            print "Accessing flash took", e - s

            # Try clean garbage
            self.env.process(
                    self.realftl.clean_garbage())


            req_index += 1

    def run(self):
        procs = []
        for i in range(self.conf['dftlncq']['ncq_depth']):
            p = self.env.process( self.process(i) )
            procs.append(p)
        e = simpy.events.AllOf(self.env, procs)
        yield e
        print "++++++++++++++++++++++++END OF FTL+++++++++++++++++++++++++++"
        print "Time:", self.env.now


