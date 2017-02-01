import datetime
from time import sleep

import pymongo
from celery.app.control import Control

from crawler import crawler, tasks
from crawler.celery import app
from crawler.db import db_mongodb as db
from crawler.settings import *


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
client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
database = client.upol_crawler

# Init database
db.init(database)

crawler.load_seed(SEED_FILE, database)

start_time = datetime.datetime.now()
last_sleep_1 = datetime.datetime.now()
sleeping = False
number_of_waiting = 0

while True:
    if sleeping is False:
        last_sleep_2 = datetime.datetime.now()

        last_sleep_delta = last_sleep_2 - last_sleep_1

        if last_sleep_delta.seconds > 5:
            sleeping = True
        else:
            sleeping = False

        if CONFIG.getboolean('Settings', 'random_unvisited_url'):
            url, value = db.get_random_url_for_crawl(database)
        else:
            url, value = db.get_url_for_crawl(database)

        if url is not None:
            print("FEEDING queue")
            db.set_queued_url(database, url)
            tasks.crawl_url_task.delay(url, value)
    else:
        print("Workers are running - SLEEPING")
        sleep(20)

        if not db.is_some_url_queued(database):
            number_of_waiting = number_of_waiting + 1
        else:
            number_of_waiting = 0

        if number_of_waiting > 5:
            break

        last_sleep_1 = datetime.datetime.now()
        sleeping = False

end_time = datetime.datetime.now()
duration = end_time - start_time

print("------------------------------")
print("Crawl FINISHED")
print("Duration: " + str(duration))
print("------------------------------")
