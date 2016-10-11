from celerytasks import app, exec_sim, add
import time

from utilities.utils import load_json

d = load_json('./varmail-localoty.json')
para = d['para_dicts'][0]
para['expname'] = 'testxxx'

# exec_sim.delay(para)
print app.conf.BROKER_URL
app.conf.BROKER_URL = 'amqp://guest@node-1//'

print app.conf.BROKER_URL

# exec_sim.delay(para)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
add.delay(8, 1000)
