from celerytasks import app, exec_sim, add, run_on_real_dev_by_para
import time
import pprint

from utilities.utils import load_json


def get_total_finished(async_ret):
    total_finished = 0
    for ret in async_ret:
        if ret.ready() == True:
            total_finished += 1

    return total_finished





# d = load_json('./varmail-localoty.json')
d = load_json('./wear-paras.json')
para_list = d['para_dicts']

app.conf.BROKER_URL = 'amqp://guest@node-0//'
app.conf.CELERY_RESULT_BACKEND = 'amqp://guest@node-0//'
print app.conf.BROKER_URL
print app.conf.CELERY_RESULT_BACKEND


total_tasks = len(para_list)

async_ret = []
for para_dict in para_list:
    if para_dict['testname'].startswith('sqliteRB'):
        for d in para_dict['appconfs']:
            d['journal_mode'] = 'DELETE'
    else:
        continue

    para_dict['expname'] = 'wear-all-apps-sqliterb-makeup7'

    # ret = exec_sim.delay(para_dict)
    ret = run_on_real_dev_by_para.delay(para_dict)
    async_ret.append(ret)


timepast = 0
while True:
    time.sleep(5)
    print get_total_finished(async_ret), '/', len(async_ret)
    timepast += 5
    print timepast



