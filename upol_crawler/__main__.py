import datetime
import shutil
import sys
from time import sleep

import pymongo
from upol_crawler import tasks
from celery.app.control import Control
from upol_crawler.core import crawler
from upol_crawler.celery import app
from upol_crawler.db import db_mongodb as db
from upol_crawler.settings import *


def main(args=None):
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
        print("WARNING: Nothing was added from seed.txt")
    else:
        db.insert_crawler_start(database)

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
    sleeping = False
    number_of_waiting = 0
    number_of_added_links = 0

    while True:
        if sleeping is False:
            batch = db.get_batch_url_for_crawl(database,
                                               int(CONFIG.get('Settings',
                                                              'db_batch_size')))

            if batch is not None:
                number_of_added_links = len(batch)
            else:
                number_of_added_links = 0

            if batch is not None:
                hashes = []

                for url in batch:
                    hashes.append(url.get('_id'))

                db.set_queued_batch(database, hashes)

                for url in batch:
                    tasks.crawl_url_task.delay(url.get('url'), url.get('depth'))

            sleeping = True
        else:
            print("------------------------------")
            print("Uptime: " + str(datetime.datetime.now() - start_time))
            print("Added links:" + str(number_of_added_links))
            print("Workers are running - SLEEPING")
            print("------------------------------")

            number_of_added_links = 0

            sleep(int(CONFIG.get('Settings', 'delay_between_feeding')))

            if not db.should_crawler_wait(database):
                number_of_waiting = number_of_waiting + 1
            else:
                number_of_waiting = 0

            if number_of_waiting >= 2:
                break

            sleeping = False

            print("FEEDING...")

    end_time = datetime.datetime.now()
    duration = end_time - start_time
    db.insert_crawler_end(database)

    print("------------------------------")
    print("Crawl FINISHED")
    print("Duration: " + str(duration))
    print("------------------------------")

if __name__ == "__main__":
    main()
