from celerytasks import app, exec_sim, add, run_on_real_dev_by_para
from commons import *
import time
import pprint

from utilities.utils import load_json


def get_total_finished(async_ret):
    total_finished = 0
    for ret in async_ret:
        if ret.ready() == True:
            total_finished += 1

    return total_finished

d = load_json('./integration-para-dicts.json')
para_list = d['para_dicts']

app.conf.BROKER_URL = 'amqp://guest@node-0//'
app.conf.CELERY_RESULT_BACKEND = 'amqp://guest@node-0//'
print app.conf.BROKER_URL
print app.conf.CELERY_RESULT_BACKEND


async_ret = []
for para_dict in para_list:
    testname = para_dict['testname']

    if 'sqlite' in testname and 'rand' in testname and para_dict['ftl'] == 'dftldes':
        para_dict['sort_block_trace'] = False
        para_dict['expname'] = 'integration-1-gc-proc-really-1'
        para_dict['stop_sim_on_bytes'] = 3*GB
        para_dict['n_channels_per_dev'] = 16
        para_dict['n_gc_procs'] = 1

        ret = exec_sim.delay(para_dict)
        async_ret.append(ret)
        # print testname, para_dict['ftl']

timepast = 0
while True:
    time.sleep(5)
    print get_total_finished(async_ret), '/', len(async_ret)
    timepast += 5
    print timepast



