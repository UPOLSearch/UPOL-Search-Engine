from __future__ import absolute_import, unicode_literals

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
        Queue(
            'logger',
            exchange=Exchange('logger'),
            routing_key='logger'
        ),
    )

    enable_utc = False
    timezone = 'Europe/Prague'
    include = ['upol_crawler.tasks']

app = Celery('upol_crawler')
app.config_from_object(Config)

if __name__ == '__main__':
    app.start()
