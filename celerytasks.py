from celery import Celery
from benchmarks.appbench import execute_simulation, run_on_real_dev


"""
To run celery:

    On worker node:
        sudo apt-get install rabbitmq-server
        sudo pip install celery
        sudo C_FORCE_ROOT=1 celery -A celerytasks worker --loglevel=info -b 'amqp://guest@node-0//'

    On pool node:
        # you should set broker to node-0 in the script
        python taskpusher.py

"""

app = Celery('tasks', broker='amqp://guest@node-0//',
        backend='amqp://guest@node-0//')

@app.task
def exec_sim(para_dict):
    execute_simulation(para_dict)

@app.task
def add(x, y):
    return x + y

@app.task
def run_on_real_dev_by_para(para):
    run_on_real_dev(para)

