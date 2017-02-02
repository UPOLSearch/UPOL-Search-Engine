import datetime
import shutil
import sys
from time import sleep

import pymongo
from celery.app.control import Control
from upol_crawler import crawler, tasks
from upol_crawler.celery import app
from upol_crawler.db import db_mongodb as db
from upol_crawler.settings import *

print("******************************")
print("UPOL-Crawler v" + CONFIG.get('Info', 'version'))
print("******************************")
print("LOADING..")

start_load_time = datetime.datetime.now()

# Start procedure
client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
database = client[DATABASE_NAME]

# Init database
db.init(database)

if crawler.load_seed(SEED_FILE, database) == 0:
    print("ERROR: Seed.txt is empty or URLs are invalid!")
    sys.exit()

end_load_time = datetime.datetime.now()

if CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
    os.makedirs(CPROFILE_DIR, exist_ok=True)
    print("Deleting cprofile folder...")
    # Cleaning cprofile folder
    shutil.rmtree(CPROFILE_DIR)

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
            url, depth = db.get_random_url_for_crawl(database)
        else:
            url, depth = db.get_url_for_crawl(database)

        if url is not None:
            number_of_added_links = number_of_added_links + 1
            db.set_queued_url(database, url)
            tasks.crawl_url_task.delay(url, depth)
    else:
        print("------------------------------")
        print("Uptime: " + str(datetime.datetime.now() - start_time))
        print("Added links:" + str(number_of_added_links))
        number_of_added_links = 0
        print("Workers are running - SLEEPING")
        print("------------------------------")

        sleep(int(CONFIG.get('Settings', 'delay_between_feeding')))

        if not db.is_some_url_queued(database):
            number_of_waiting = number_of_waiting + 1
        else:
            number_of_waiting = 0

        if number_of_waiting > 2:
            break

        last_sleep_1 = datetime.datetime.now()
        sleeping = False

end_time = datetime.datetime.now()
duration = end_time - start_time

print("------------------------------")
print("Crawl FINISHED")
print("Duration: " + str(duration))
print("------------------------------")
