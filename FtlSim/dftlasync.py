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


class FTL(ftlbuilder.FtlBuilder):
    """
    The interface of this FTL for the host is a queue (NCQ). The host puts
    requests to the queue with certain time intervals according to the
    blktrace.
    """
    def __init__(self, confobj, recorderobj, flashobj, simpy_env):
        super(FTL, self).__init__(confobj, recorderobj, flashobj)

        self.env = simpy_env

        self.ncq = NCQSingleQueue(
                ncq_depth = self.conf['dftlasync']['ncq_depth'],
                simpy_env = self.env)

        self.flash_controller = flashcontroller.controller.Controller(
                self.env, self.conf)

    def process(self):
        req_index = 0
        while True:
            io_req = yield self.ncq.queue.get()
            print io_req.operation, ', request index', req_index
            print 'Got request (', req_index, ') at time', self.env.now
            req_index += 1

            # create a bunch of requests to the flash here
            sub_reqs = []
            for i in range(3):
                sub_reqs.append(
                        create_request(channel = i, op = 'read'))

            # send flash requests to flash controller
            ctrl_procs = []
            for sub_req in sub_reqs:
                p = self.env.process(
                        self.flash_controller.execute_request(sub_req))
                ctrl_procs.append(p)

            all_ctrl_procs = simpy.events.AllOf(self.env, ctrl_procs)
            yield all_ctrl_procs
            print 'Finish request (', req_index, ') at time', self.env.now






