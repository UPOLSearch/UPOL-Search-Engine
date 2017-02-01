import datetime
from time import sleep

import pymongo
from celery.app.control import Control

from crawler import crawler, tasks
from crawler.celery import app
from crawler.db import db_mongodb as db
from crawler.settings import *

print("******************************")
print("UPOL-Crawler v" + CONFIG.get('Info', 'version'))
print("******************************")
print("LOADING..")

start_load_time = datetime.datetime.now()

# Start procedure
client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
database = client.upol_crawler

# Init database
db.init(database)

crawler.load_seed(SEED_FILE, database)

end_load_time = datetime.datetime.now()

print("DONE! " + str(end_load_time - start_load_time))
print("------------------------------")
print("Start crawling...")
print("******************************")

start_time = datetime.datetime.now()
last_sleep_1 = datetime.datetime.now()
sleeping = False
number_of_waiting = 0
number_of_added_links = 0

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
            number_of_added_links = number_of_added_links + 1
            db.set_queued_url(database, url)
            tasks.crawl_url_task.delay(url, value)
    else:
        print("------------------------------")
        print("Added links:" + str(number_of_added_links))
        number_of_added_links = 0
        print("Workers are running - SLEEPING")
        print("------------------------------")
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
