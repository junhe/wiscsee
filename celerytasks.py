from celery import Celery

app = Celery('tasks', broker='amqp://guest@localhost//')

@app.task
def pass_dict(para_dict):
        return para_dict
