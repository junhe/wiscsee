from celery import Celery
from benchmarks.appbench import execute_simulation

app = Celery('tasks', broker='amqp://guest@localhost//')

@app.task
def exec_sim(para_dict):
    execute_simulation(para_dict)

