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

class NativeCommandQueue(object):
    """
    The NCQ is implemented as the tail of several queues.

                        NCQ
    Q1: ****************#
    Q2:      ***********#
    Q3:   **************#

    The trace replayer puts requests to the Q1, Q2, and Q3.
    The FTL takes requests out from the other end (#).
    """
    def __init__(self, ncq_depth, simpy_env):
        self.ncq_depth = ncq_depth
        self.env = simpy_env
        self.queues = [ deque() for _ in range(self.ncq_depth) ]
        self.req_container = simpy.Container(self.env)

    def pop(self, index):
        """
        Pop from the right of one of the subqueues.
        You have to make sure queues[index] is not empty before calling.
        """
        self.req_container.get(1)
        return self.queues[index].pop()

    def peek_tail(self, index):
        """
        Return the last element of queues[index] without modifying the queues
        """
        return self.queues[index][-1]

    def push(self, element, index):
        """
        Put one element to queue of index.
        """
        self.req_container.put(1)
        self.queues[index].appendleft(element)

    def is_empty(self, index):
        return len(self.queues[index]) == 0


class FTLNCQ(NativeCommandQueue):
    """
    This class has additional functions for FTL operations
    """
    def __init__(self, ncq_depth, simpy_env):
        super(FTLNCQ, self).__init__(ncq_depth, simpy_env)

        self.next_queue = 0
        self.requests_coming = True

    def next_request(self):
        """
        iterate the ncq to get the next request.
        it can be more sophisticated later
        """
        for i in range(self.ncq_depth):
            # Try each queue once and only once
            queue_i = (i + self.ncq_depth) % self.ncq_depth
            if self.is_empty(index = queue_i):
                continue
            else:
                self.next_queue = (queue_i + 1) % self.ncq_depth
                return self.queues[queue_i].pop()
        return None


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

    def process(self):
        req_index = 0
        while True:
            io_req = yield self.ncq.queue.get()
            print io_req.operation, ', request index', req_index
            req_index += 1


