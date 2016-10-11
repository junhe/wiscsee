from celerytasks import app, exec_sim, add
import time

from utilities.utils import load_json

d = load_json('./varmail-localoty.json')
para = d['para_dicts'][0]
para['expname'] = 'testxxx'

# exec_sim.delay(para)
print app.conf.BROKER_URL
app.conf.BROKER_URL = 'amqp://guest@node-0//'
app.conf.CELERY_RESULT_BACKEND = 'rpc://guest@node-0//'

print app.conf.BROKER_URL

# exec_sim.delay(para)
ret = add.delay(8, 10003333)

print ret.ready()
time.sleep(1)
print ret.ready()



