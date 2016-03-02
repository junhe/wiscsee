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
from ftlsim_commons import *
import datacache
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


class SSDFramework(object):
    """
    The interface of this FTL for the host is a queue (NCQ). The host puts
    requests to the queue with certain time intervals according to the
    blktrace.

    realftl should provide translate() process to take host request and
    translate them to flash access requests
    realftl should provide clean_garbage()

    """
    def __init__(self, confobj, recorderobj, simpy_env):
        self.conf = confobj
        self.recorder = recorderobj
        self.env = simpy_env

        self.ncq = NCQSingleQueue(
                ncq_depth = self.conf['SSDFramework']['ncq_depth'],
                simpy_env = self.env)

        self.flash_controller = flashcontroller.controller.Controller3(
                self.env, self.conf, self.recorder)

        self.datacache = datacache.DataCache(
            self.conf['SSDFramework']['data_cache_max_n_entries'], self.env)

        if self.conf['ftl_type'] == 'dftldes':
            self.realftl = dftldes.Dftl(self.conf, self.recorder,
                    self.flash_controller, self.env)
        else:
            raise RuntimeError("ftl_type {} is not supported.".format(
                self.conf['ftl_type']))

        self.recorder.disable()

    def get_direct_mapped_flash_requests(self, host_req):
        """
        For example, read LPN 20 will executed as reading PPN 20

        This is only for debugging.
        """
        page_start, page_count = self.conf.sec_ext_to_page_ext(host_req.sector,
                host_req.sector_count)
        if host_req.operation == 'discard':
            return []
        elif host_req.operation in ('read', 'write'):
            return self.flash_controller.get_flash_requests_for_ppns(
                    page_start, page_count, op = host_req.operation)
        elif host_req.operation in ('enable_recorder', 'disable_recorder'):
            return []
        else:
            raise RuntimeError("operation {} is not supported".format(
                host_req.operation))

    def access_flash(self, flash_reqs):
        """
        This simpy process spawn multiple processes to access flash
        Some processes may be stalled if channel is busy
        """
        ctrl_procs = []
        for flash_req in flash_reqs:
            # This will have the requests automatically queued at channel
            p = self.env.process(
                    self.flash_controller.execute_request(flash_req,
                        tag = TAG_FOREGROUND))
            ctrl_procs.append(p)

        all_ctrl_procs = simpy.events.AllOf(self.env, ctrl_procs)
        yield all_ctrl_procs

    def extent_to_request_list(self, subextents, operation):
        req_list = []
        for subextent in subextents:
            ssd_req = SSDRequest(
                    subextent.lpn_start,
                    subextent.lpn_count,
                    subextent.in_cache,
                    operation)
            req_list.appen(ssd_req)

        return req_list

    def release_token(self, event):
        event.token.release(event.token_req)

    def process(self, pid):
        req_index = 0
        while True:
            host_event = yield self.ncq.queue.get()

            if host_event.operation == 'end_process':
                self.release_token(host_event)
                break
            elif host_event.operation == 'enable_recorder':
                self.realftl.recorder.enable()
                self.workload_start_time = self.env.now
                self.recorder.add_to_timer("workload_start_time", 0, self.env.now)

                self.release_token(host_event)
                continue
            elif not host_event.operation in ('read', 'write', 'discard'):
                self.release_token(host_event)
                continue

            ssd_req = create_ssd_request(self.conf, host_event)

            s = self.env.now
            flash_reqs = yield self.env.process(
                    self.realftl.translate(ssd_req, pid) )
            e = self.env.now
            self.recorder.add_to_timer("translation_time-w_wait", pid,
                    e - s)

            s = self.env.now
            yield self.env.process(
                    self.access_flash(flash_reqs))
            e = self.env.now
            self.recorder.add_to_timer("forground_flash_access_time-w_wait", pid,
                    e - s)

            # Try clean garbage
            self.env.process(
                    self.realftl.clean_garbage())

            req_index += 1

            if pid == 0 and req_index % 100 == 0:
                print self.env.now / float(SEC)

            self.release_token(host_event)

    def run(self):
        procs = []
        for i in range(self.conf['SSDFramework']['ncq_depth']):
            p = self.env.process( self.process(i) )
            # p = self.env.process( self.data_cache_process(i) )
            procs.append(p)
        e = simpy.events.AllOf(self.env, procs)
        yield e
        print "++++++++++++++++++++++++END OF FTL+++++++++++++++++++++++++++"
        print "Time:", float(self.env.now)
        print "Workload time", float(self.env.now - self.workload_start_time) / SEC
        self.recorder.add_to_timer("simulation_time", 0, self.env.now)



