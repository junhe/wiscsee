from celerytasks import app, exec_sim, add
import time

from utilities.utils import load_json


def get_total_finished(async_ret):
    total_finished = 0
    for ret in async_ret:
        if ret.ready() == True:
            total_finished += 1

    return total_finished





d = load_json('./varmail-localoty.json')
para_list = d['para_dicts']

app.conf.BROKER_URL = 'amqp://guest@node-0//'
app.conf.CELERY_RESULT_BACKEND = 'rpc://guest@node-0//'
print app.conf.BROKER_URL
print app.conf.CELERY_RESULT_BACKEND


total_tasks = len(para_list)

async_ret = []
for para_dict in para_list:
    ret = exec_sim.delay(para_dict)
    async_ret.append(ret)


timepast = 0
while True:
    time.sleep(5)
    print get_total_finished(async_ret), '/', total_tasks
    timepast += 5
    print timepast



