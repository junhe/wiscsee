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
import hostevent
import lrulist
import recorder
from utilities import utils
import dftldes
from workrunner import blocktrace


class SsdBase(object):
    def _process(self, pid):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()


class Ssd(SsdBase):
    def __init__(self, conf, simpy_env, ncq, rec_obj):
        self.conf = conf
        self.env = simpy_env
        self.recorder = rec_obj
        self.ncq = ncq # should be initialized in Simulator
        self.n_processes = self.ncq.ncq_depth

        self.flash_controller = flashcontroller.controller.Controller3(
                self.env, self.conf, self.recorder)

        self.ftl = eval("{}.Ftl(self.conf, self.recorder, self.flash_controller, "
                "self.env)".format(self.conf['ftl_type']))

    def _process(self, pid):
        for req_i in itertools.count():
            host_event = yield self.ncq.queue.get()

            slot_req = self.ncq.slots.request()
            yield slot_req

            # handle host_event case by case
            operation = host_event.operation

            if operation == 'enable_recorder':
                self.recorder.enable()
            elif operation == 'shut_ssd':
                print 'got shut_ssd'
                sys.stdout.flush()
                yield self.env.process(
                    self._end_all_processes())
            elif operation == 'end_ssd_process':
                self.ncq.slots.release(slot_req)
                break
            elif operation == 'read':
                yield self.env.process(
                    self.ftl.read_ext(host_event.get_lpn_extent(self.conf)))
            elif  operation == 'write':
                yield self.env.process(
                    self.ftl.write_ext(host_event.get_lpn_extent(self.conf)))
            elif  operation == 'discard':
                yield self.env.process(
                    self.ftl.discard_ext(host_event.get_lpn_extent(self.conf)))
            else:
                raise NotImplementedError("Operation {} not supported."\
                        .format(host_event.operation))

            if req_i % 1000 == 0:
                print '.',
                sys.stdout.flush()

            if self.ftl.is_cleaning_needed() is True:
                pass
                self.env.process(self._cleaner_process())

            self.ncq.slots.release(slot_req)

    def _end_all_processes(self):
        for i in range(self.n_processes):
            yield self.ncq.queue.put(
                hostevent.EventSimple(0, "end_ssd_process"))

    def _cleaner_process(self):
        held_slot_reqs = yield self.env.process(self.ncq.hold_all_slots())

        # things may have changed since last time we check, because of locks
        if self.ftl.is_cleaning_needed():
            yield self.env.process(self.ftl.clean())

        self.ncq.release_all_slots(held_slot_reqs)

    def run(self):
        procs = []
        for i in range(self.n_processes):
            p = self.env.process( self._process(i) )
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)


        self._record_post_run_stats()

    def _record_post_run_stats(self):
        self.recorder.set_result_by_one_key(
                'simulation_duration', self.env.now)
        print self.recorder.get_result_summary()










































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

    def release_token(self, event):
        event.token.release(event.token_req)

    def process(self, pid):
        for req_index in itertools.count():
            host_event = yield self.ncq.queue.get()

            if host_event.operation == 'shut_ssd':
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

        self.record_blkparse_bw()
        self.print_statistics()

    def record_blkparse_bw(self):
        raw_blkparse_file_path = os.path.join(
                self.conf['result_dir'], 'blkparse-output.txt')

        if not os.path.exists(raw_blkparse_file_path):
            return

        blkresult = blocktrace.BlktraceResult(self.conf,
                raw_blkparse_file_path, None)
        self.recorder.set_result_by_one_key(
                'blkparse_read_bw',
                blkresult.get_bandwidth_mb('read'))
        self.recorder.set_result_by_one_key(
                'blkparse_write_bw',
                blkresult.get_bandwidth_mb('write'))
        self.recorder.set_result_by_one_key(
                'blkparse_duration',
                blkresult.get_duration())

    def print_statistics(self):
        print '++++++++++++++++++++ statistics ++++++++++++++++++'
        print 'sim duration', self.recorder.result_dict['simulation_duration']
        print 'workload start', self.recorder.result_dict['workload_start_time']
        print 'workload duration', \
            self.recorder.result_dict['simulation_duration'] - \
            self.recorder.result_dict['workload_start_time']
        print 'blkparse_duration', self.recorder.result_dict.get(
                'blkparse_duration', None)



