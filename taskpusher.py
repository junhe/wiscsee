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
    testname = para_dict['testname']

    # skip non-sqlite
    if not (testname.startswith('sqliteRB') or testname.startswith('sqliteWAL')):
        continue

    # set sqliteRB and sqliteWAL to both have smaller insertions
    for d in para_dict['appconfs']:
        d['n_insertions'] = 60000000
    para_dict['sort_block_trace'] = False
    para_dict['do_dump_lpn_sem'] = False

    # set sqliteRB to be really sqliteRB
    if testname.startswith('sqliteRB'):
        for d in para_dict['appconfs']:
            d['journal_mode'] = 'DELETE'


    para_dict['expname'] = 'wear-sqliterb-sqlitewal-makeup-nosem2'

    ret = run_on_real_dev_by_para.delay(para_dict)
    async_ret.append(ret)

timepast = 0
while True:
    time.sleep(5)
    print get_total_finished(async_ret), '/', len(async_ret)
    timepast += 5
    print timepast



