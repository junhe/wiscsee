import bitarray
from collections import deque, Counter
import csv
import datetime
import itertools
import random
import os
import pprint
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
from WlRunner import blocktrace


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
        for req_index in itertools.count():
            host_event = yield self.ncq.queue.get()

            if host_event.operation == 'end_process':
                self.release_token(host_event)
                break

            if host_event.operation == 'enable_recorder':
                self.realftl.recorder.enable()
                self.recorder.set_result_by_one_key('workload_start_time',
                        self.env.now)

                self.release_token(host_event)
                continue
            elif not host_event.operation in ('read', 'write', 'discard'):
                self.release_token(host_event)
                continue

            ssd_req = create_ssd_request(self.conf, host_event)

            # Translate for the reqests
            s = self.env.now
            flash_reqs = yield self.env.process(
                    self.realftl.translate(ssd_req, pid) )
            e = self.env.now
            self.recorder.add_to_timer("translation_time-w_wait", pid,
                    e - s)

            # Access flash
            s = self.env.now
            yield self.env.process(
                    self.access_flash(flash_reqs))
            e = self.env.now
            self.recorder.add_to_timer("forground_flash_access_time-w_wait", pid,
                    e - s)

            # Try clean garbage
            self.env.process(
                    self.realftl.clean_garbage())

            if pid == 0 and req_index % 100 == 0:
                print self.env.now / float(SEC)

            self.release_token(host_event)

    def run(self):
        procs = []
        for i in range(self.conf['SSDFramework']['ncq_depth']):
            p = self.env.process( self.process(i) )
            procs.append(p)
        e = simpy.events.AllOf(self.env, procs)
        yield e

        self.recorder.set_result_by_one_key(
                "simulation_duration", self.env.now)

        self.record_last_timestamp()
        self.print_statistics()

    def record_last_timestamp(self):
        raw_blkparse_file_path = os.path.join(
                self.conf['result_dir'], 'blkparse-output.txt')

        if not os.path.exists(raw_blkparse_file_path):
            return

        rawparser = blocktrace.RawParser(self.conf,
                raw_blkparse_file_path, None)
        last_timestamp = rawparser.parse_raw()[-1]['timestamp']
        self.recorder.set_result_by_one_key('last_blkparse_timestamp',
            last_timestamp)

    def print_statistics(self):
        print '++++++++++++++++++++ statistics ++++++++++++++++++'
        print 'sim duration', self.recorder.result_dict['simulation_duration']
        print 'workload start', self.recorder.result_dict['workload_start_time']
        print 'workload duration', \
            self.recorder.result_dict['simulation_duration'] - \
            self.recorder.result_dict['workload_start_time']
        print 'last_timestamp', self.recorder.result_dict.get(
                'last_blkparse_timestamp', None)



