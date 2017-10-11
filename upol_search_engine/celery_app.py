from celery import Celery
from celery.schedules import crontab
from kombu import Exchange, Queue
from upol_search_engine import settings


class Config(object):
    broker_url = 'amqp://guest:guest@localhost:5672//'
    result_backend = 'amqp://guest:guest@localhost:5672//'

    task_queues = (
        Queue(
            'crawler',
            exchange=Exchange('crawler'),
            routing_key='crawler'
        ),
        Queue(
            'search_engine',
            exchange=Exchange('search_engine'),
            routing_key='search_engine'
        ),
    )

    enable_utc = False
    timezone = 'Europe/Prague'
    include = ['upol_search_engine.upol_crawler.tasks',
               'upol_search_engine.tasks']

    log_file = settings.CONFIG.get('Settings', 'log_dir')
    task_acks_late = True

    beat_schedule = {
        'run-search-engine': {
            'task': 'upol_search_engine.tasks.main_task',
            'schedule': crontab(minute='3'),
            'args': ()
        }
    }


app = Celery('celery_app')
app.config_from_object(Config)

if __name__ == '__main__':
    app.start()
