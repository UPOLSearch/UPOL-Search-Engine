from __future__ import absolute_import, unicode_literals
from celery import Celery
from kombu import Queue, Exchange

class Config(object):
    broker_url = "amqp://guest:guest@localhost:5672//"

    task_queues = (
        Queue(
            'important2',
            exchange=Exchange('important'),
            routing_key="important",
            queue_arguments={'x-max-length': 4}
        ),
    )

    enable_utc = True
    timezone = 'Europe/Prague'
    include=['crawler.tasks']

app = Celery('crawler')
app.config_from_object(Config)
            #  broker='redis://localhost:6379/0',
            #  # backend='amqp://',
            #  include=['crawler.tasks'],
            #  timezone='Europe/Prague')

# # Optional configuration, see the application user guide.
# app.conf.update(
#     result_expires=3600,
# )

if __name__ == '__main__':
    app.start()
