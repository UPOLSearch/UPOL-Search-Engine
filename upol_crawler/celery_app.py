# from __future__ import absolute_import

from celery import Celery
from kombu import Exchange, Queue


class Config(object):
    broker_url = 'amqp://guest:guest@localhost:5672//'

    task_queues = (
        Queue(
            'crawler',
            exchange=Exchange('crawler'),
            routing_key='crawler'
        ),
        # Queue(
        #     'collector',
        #     exchange=Exchange('collector'),
        #     routing_key='collector'
        # ),
    )

    enable_utc = False
    timezone = 'Europe/Prague'
    include = ['upol_crawler.tasks']
    worker_hijack_root_logger = False
    task_acks_late = True

app = Celery('celery_app')
app.config_from_object(Config)

if __name__ == '__main__':
    app.start()
