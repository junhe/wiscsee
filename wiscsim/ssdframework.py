import bitarray
from collections import deque, Counter
import datetime
import itertools
import random
import os
import pprint
import Queue
import sys
import simpy
import copy

import bidict

import config
from commons import *
from ftlsim_commons import *
import flash
import controller
import ftlbuilder
import hostevent
import lrulist
import recorder
from utilities import utils
import dftldes
import nkftl2

from pyreuse.sysutils import blocktrace, blockclassifiers, dumpe2fsparser

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

        self.flash_controller = controller.Controller3(
                self.env, self.conf, self.recorder)

        print 'initializing ssd...........', self.conf['ftl_type']

        self.ftl = self._create_ftl()

        self._snapshot_valid_ratios = self.conf['snapshot_valid_ratios']
        self._snapshot_erasure_count_dist = self.conf['snapshot_erasure_count_dist']
        self._snapshot_interval = self.conf['snapshot_interval']
        self._snapshot_user_traffic = True

        self._do_wear_leveling = self.conf['do_wear_leveling']
        self._wear_leveling_check_interval = self.conf['wear_leveling_check_interval']

        self.gc_sleep_timer = 0
        self.gc_sleep_duration = 10

    def _create_ftl(self):
        if self.conf['ftl_type'] == 'dftldes':
            return dftldes.Ftl(self.conf, self.recorder, self.flash_controller,
                self.env)
        elif self.conf['ftl_type'] == 'nkftl2':
            print 'we will use nkftl2'
            simpleflash = flash.Flash(recorder=self.recorder, confobj=self.conf)
            return nkftl2.Ftl(self.conf, self.recorder, simpleflash, self.env,
                    self.flash_controller)

    def _barrier(self):
        """
        Grab and hold the rest of the ncq slots (already holding one)
        """
        reqs = []
        for i in range(self.ncq.ncq_depth - 1):
            slot_req = self.ncq.slots.request()
            reqs.append(slot_req)
        yield simpy.AllOf(self.env, reqs)

        for req in reqs:
            self.ncq.slots.release(req)

    def _process(self, pid):
        for req_i in itertools.count():
            host_event = yield self.ncq.queue.get()

            slot_req = self.ncq.slots.request()
            yield slot_req

            # handle host_event case by case
            operation = host_event.get_operation()

            if operation == OP_ENABLE_RECORDER:
                self.recorder.enable()

            elif operation == OP_DISABLE_RECORDER:
                self.recorder.disable()

            elif operation == OP_WORKLOADSTART:
                pass

            elif operation == OP_SHUT_SSD:
                print 'got shut_ssd'
                sys.stdout.flush()
                yield self.env.process(self._end_all_processes())

            elif operation == OP_BARRIER:
                # The correct way of doing barrier is to use
                # OP_BARRIER with more than n_ncq_slots OP_NOOP.
                # the OP_NOOPs are to make sure no other operations
                # can be scheduled before OP_BARRIER.
                # for example, EventA, OP_BARRIER, EventB cannot garantee
                # that B executed before A because all A, Barrier and B can
                # get the slots at the same time. Schedule is free to run
                # them in any order. EventA, OP_NOOP x 9999, EventB cannot
                # garantee either because OP_NOOP takes no time and Event B
                # can run at the same time as A..
                # So the correct way is
                # EventA, OP_NOOPsx9999, OP_BARRIER, OP_NOOPSX9999, EventB
                # OP_BARRIER will wait util EventA finishes. The first OP_NOOP
                # x9999 makes sure eventA get a slot before barrier

                yield self.env.process(self._barrier())

            elif operation == OP_NOOP:
                pass

            elif operation == OP_CALC_GC_DURATION:
                dur = self.recorder.get_result_by_one_key('gc_end') - \
                        self.recorder.get_result_by_one_key('gc_start')
                self.recorder.set_result_by_one_key('gc_duration', dur)
                self.recorder.set_result_by_one_key('gc_duration_sec', dur/SEC)

            elif operation == OP_CALC_NON_MERGE_GC_DURATION:
                dur = self.recorder.get_result_by_one_key('non_merge_gc_end') - \
                        self.recorder.get_result_by_one_key('non_merge_gc_start')
                self.recorder.set_result_by_one_key('non_merge_gc_duration', dur)
                self.recorder.set_result_by_one_key('non_merge_gc_duration_sec', dur/SEC)

            elif operation == OP_FLUSH_TRANS_CACHE:
                if self.conf['ftl_type'] == 'dftldes':
                    yield self.env.process(self.ftl.flush_trans_cache())

            elif operation == OP_PURGE_TRANS_CACHE:
                if self.conf['ftl_type'] == 'dftldes':
                    yield self.env.process(self.ftl.purge_trans_cache())

            elif operation == OP_DROP_TRANS_CACHE:
                if self.conf['ftl_type'] == 'dftldes':
                    self.ftl.drop_trans_cache()

            elif operation == OP_REC_TIMESTAMP:
                self.recorder.set_result_by_one_key(host_event.arg1,
                        self.env.now)

            elif operation == OP_REC_FLASH_OP_CNT:
                result_dict = self.recorder.get_result_summary()
                flashops = copy.deepcopy(
                    result_dict['general_accumulator'].get('flash_ops', {}))
                self.recorder.set_result_by_one_key(host_event.arg1,
                        flashops)

            elif operation == OP_REC_FOREGROUND_OP_CNT:
                result_dict = self.recorder.get_result_summary()
                traffic = copy.deepcopy(
                    result_dict['general_accumulator'].get('traffic', {}))
                self.recorder.set_result_by_one_key(host_event.arg1,
                        traffic)

            elif operation == OP_REC_CACHE_HITMISS:
                result_dict = self.recorder.get_result_summary()
                data = copy.deepcopy(
                    result_dict['general_accumulator'].get('Mapping_Cache', {}))
                self.recorder.set_result_by_one_key(host_event.arg1,
                        data)

            elif operation == OP_END_SSD_PROCESS:
                self.ncq.slots.release(slot_req)
                break

            elif operation == OP_CLEAN:
                print 'start cleaning'
                yield self.env.process(self._cleaner_process(forced=True))

            elif operation == OP_REC_BW:
                dur = self.recorder.get_result_by_one_key('interest_workload_end') - \
                        self.recorder.get_result_by_one_key('interest_workload_start')
                self.recorder.set_result_by_one_key('workload_duration_nsec', dur)
                self.recorder.set_result_by_one_key('workload_duration_sec', float(dur)/SEC)

                write_traffic = self.recorder.get_general_accumulater_cnt(
                        'traffic', 'write') / MB

                self.recorder.set_result_by_one_key('workload_duration_nsec', dur)

                if dur == 0:
                    write_bw = "NA"
                else:
                    write_bw = float(write_traffic)/(float(dur) / SEC)

                self.recorder.set_result_by_one_key('write_bandwidth', write_bw)
                print '>>>>>>>>>> Bandwidth (MB/s) <<<<<<<<<<<', write_bw
                print '>>>>>>>>>> Traffic (MB)     <<<<<<<<<<<', write_traffic
                print '>>>>>>>>>> Duration (sec)   <<<<<<<<<<<', float(dur) / SEC

            elif operation == OP_NON_MERGE_CLEAN:
                print 'start non merge cleaning'
                if self.conf['ftl_type'] == 'nkftl2':
                    yield self.env.process(self.ftl.clean(forced=True, merge=False))

            elif operation == OP_READ:
                yield self.env.process(
                    self.ftl.read_ext(host_event.get_lpn_extent(self.conf)))

            elif  operation == OP_WRITE:
                yield self.env.process(
                    self.ftl.write_ext(host_event.get_lpn_extent(self.conf)))

            elif  operation == OP_DISCARD:
                yield self.env.process(
                    self.ftl.discard_ext(host_event.get_lpn_extent(self.conf)))

            elif operation in [OP_FALLOCATE]:
                pass

            else:
                raise NotImplementedError("Operation {} not supported."\
                        .format(host_event.operation))

            if req_i % 1000 == 0:
                print '.',
                sys.stdout.flush()

            if self.gc_sleep_timer > 0:
                self.gc_sleep_timer -= 1

            if self.gc_sleep_timer == 0 and self.ftl.is_cleaning_needed() is True:
                yield self.env.process(self._cleaner_process())
                # if we just did gc, we disable the next X gc checks
                # so don't try gc for every request.
                # This also gives it time to generate garbage with low
                # valid ratio
                self.gc_sleep_timer = self.gc_sleep_duration

            self.ncq.slots.release(slot_req)

    def _end_all_processes(self):
        for i in range(self.n_processes):
            yield self.ncq.queue.put(
                hostevent.ControlEvent(OP_END_SSD_PROCESS))
        self._snapshot_valid_ratios = False
        self._snapshot_erasure_count_dist = False
        self._do_wear_leveling = False
        self._snapshot_user_traffic = False

    def _cleaner_process(self, forced=False):
        # things may have changed since last time we check, because of locks
        if forced is True or self.ftl.is_cleaning_needed():
            yield self.env.process(self.ftl.clean(forced))

    def _wear_leveling_process(self):
        print 'wear leveling process start'
        while self._do_wear_leveling is True:
            yield self.env.timeout(self._wear_leveling_check_interval)
            if self.ftl.is_wear_leveling_needed() is True:
                print 'start wear leveling...'
                yield self.env.process(self.ftl.level_wear())
            else:
                print 'skip wear leveling'
        print 'wear leveling process ends'


    def _valid_ratio_snapshot_process(self):
        while self._snapshot_valid_ratios is True:
            self.ftl.snapshot_valid_ratios()
            yield self.env.timeout(self._snapshot_interval)

    def _user_traffic_size_snapshot_process(self):
        while self._snapshot_user_traffic is True:
            self.ftl.snapshot_user_traffic()
            yield self.env.timeout(0.1*SEC)

    def _erasure_count_dist_snapshot_process(self):
        while self._snapshot_erasure_count_dist is True:
            self.ftl.snapshot_erasure_count_dist()
            yield self.env.timeout(self._snapshot_interval)

    def run(self):
        procs = []
        for i in range(self.n_processes):
            p = self.env.process( self._process(i) )
            procs.append(p)

        p = self.env.process( self._valid_ratio_snapshot_process() )
        procs.append(p)

        p = self.env.process( self._erasure_count_dist_snapshot_process() )
        procs.append(p)

        p = self.env.process( self._wear_leveling_process() )
        procs.append(p)

        p = self.env.process( self._user_traffic_size_snapshot_process() )
        procs.append(p)

        yield simpy.events.AllOf(self.env, procs)








































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

        self.flash_controller = controller.Controller3(
                self.env, self.conf, self.recorder)

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

            if host_event.operation == OP_SHUT_SSD:
                self.release_token(host_event)
                break

            if host_event.operation == OP_ENABLE_RECORDER:
                self.realftl.recorder.enable()
                self.recorder.set_result_by_one_key('workload_start_time',
                        self.env.now)

                self.release_token(host_event)
                continue
            elif not host_event.operation in (OP_READ, OP_WRITE, OP_DISCARD):
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

        blkresult = blocktrace.BlktraceResult(
                self.conf['sector_size'],
                self.conf['event_file_column_names'],
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



