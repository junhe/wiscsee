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
import flash
import flashcontroller
import ftlbuilder
import lrulist
import recorder
import utils


class NCQSingleQueue(object):
    """
    User of the queue can take up to depth # of request without
    returning
    """
    def __init__(self, ncq_depth, simpy_env):
        self.ncq_depth = ncq_depth
        self.env = simpy_env
        self.queue = simpy.Store(self.env)


def create_request(channel, op):
    req = flashcontroller.controller.FlashRequest()

    req.addr = flashcontroller.controller.FlashAddress()
    req.addr.channel = channel
    if op == 'read':
        req.operation = flashcontroller.controller.FlashRequest.OP_READ
    elif op == 'write':
        req.operation = flashcontroller.controller.FlashRequest.OP_WRITE
    elif op == 'erase':
        req.operation = flashcontroller.controller.FlashRequest.OP_ERASE
    else:
        raise RuntimeError()

    return req


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
            p = self.env.process(
                    self.flash_controller.execute_request(flash_req))
            ctrl_procs.append(p)

        all_ctrl_procs = simpy.events.AllOf(self.env, ctrl_procs)
        yield all_ctrl_procs

    def process(self, pid):
        req_index = 0
        while True:
            io_req = yield self.ncq.queue.get()
            print pid, 'Got request (', req_index, io_req.operation, ') at time', self.env.now

            flash_reqs = self.get_direct_mapped_flash_requests(io_req)

            yield self.env.process(
                    self.access_flash(flash_reqs))
            print pid, 'Finish request (', req_index, io_req.operation, ') at time', self.env.now

            req_index += 1

    def run(self):
        for i in range(self.conf['dftlncq']['ncq_depth']):
            self.env.process( self.process(i) )
        # no need to wait for all processes to finish here because
        # this function is not a simpy process


