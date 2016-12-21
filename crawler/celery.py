from __future__ import absolute_import, unicode_literals
from celery import Celery
from kombu import Queue, Exchange


class Config(object):
    broker_url = "amqp://guest:guest@localhost:5672//"

    task_queues = (
        Queue(
            'crawler',
            exchange=Exchange('crawler'),
            routing_key="crawler",
            queue_arguments={'x-max-length': 3000}
        ),
        # Queue(
        #     'logger',
        #     exchange=Exchange('logger'),
        #     routing_key="logger",
        #     queue_arguments={'x-max-length': 3000}
        # ),
    )

    enable_utc = False
    timezone = 'Europe/Prague'
    include = ['crawler.tasks']

app = Celery('crawler')
app.config_from_object(Config)

if __name__ == '__main__':
    app.start()
