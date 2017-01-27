from crawler import tasks
from crawler.db import db_mongodb as db
from crawler import config
import pymongo
from time import sleep
import datetime
from crawler import crawler

from celery.app.control import Control
from crawler.celery import app


def is_worker_running():
    inspect = app.control.inspect()

    active = inspect.active()
    scheduled = inspect.scheduled()
    reserved = inspect.reserved()

    if active is not None:
        active_number = len(list(active.values())[0])
    else:
        active_number = 0
    if scheduled is not None:
        scheduled_number = len(list(scheduled.values())[0])
    else:
        scheduled_number = 0
    if reserved is not None:
        reserved_number = len(list(reserved.values())[0])
    else:
        reserved_number = 0

    if active_number + scheduled_number + reserved_number > 0:
        return True
    else:
        return False

# Start procedure
database = db.database

# Init database
db.init(database)

crawler.load_seed(config.seed_path)

start_time = datetime.datetime.now()
sleeping = False
number_of_tasks = 0

while True:
    if sleeping is False:
        url, value = db.get_unvisited_url(database)

        if url is not None:
            print("FEEDING QUEUE")
            db.set_visited_url(database, url)
            tasks.crawl_url_task.delay(url, value)

        else:
            print("WORKER IS RUNNING - SLEEPING")
