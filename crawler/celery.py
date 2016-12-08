from __future__ import absolute_import, unicode_literals
from celery import Celery

app = Celery('crawler',
             broker='redis://localhost:6379/0',
             # backend='amqp://',
             include=['crawler.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    # Here you will place the code
    app.start()
