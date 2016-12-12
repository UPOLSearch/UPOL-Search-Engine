from __future__ import absolute_import, unicode_literals
from celery import Celery
from kombu import Queue, Exchange


class Config(object):
    broker_url = "amqp://guest:guest@localhost:5672//"

    task_queues = (
        Queue(
            'important',
            exchange=Exchange('important'),
            routing_key="important",
            queue_arguments={'x-max-length': 100}
        ),
    )

    enable_utc = True
    timezone = 'Europe/Prague'
    include = ['crawler.tasks']

app = Celery('crawler')
app.config_from_object(Config)

if __name__ == '__main__':
    app.start()
