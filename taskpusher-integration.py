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

    # skip read until we also record read traffic size
    # if '_r_' in testname:
        # continue

    para_dict['sort_block_trace'] = False
    para_dict['expname'] = 'integration-all-180'
    para_dict['stop_sim_on_bytes'] = 'inf'

    ret = exec_sim.delay(para_dict)
    async_ret.append(ret)

timepast = 0
while True:
    time.sleep(5)
    print get_total_finished(async_ret), '/', len(async_ret)
    timepast += 5
    print timepast



