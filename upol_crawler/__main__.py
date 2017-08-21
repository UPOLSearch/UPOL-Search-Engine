import shutil
import sys
from datetime import datetime
from time import sleep

import pymongo
from upol_crawler import db, tasks
from upol_crawler.celery_app import app
from upol_crawler.core import crawler, validator
from upol_crawler.settings import *
from upol_crawler.tools import logger

log = logger.universal_logger('feeder')

def should_crawler_wait(db):
    """Check if crawler can terminate or not"""
    result = db['Urls'].find_one({'$or': [
        {'$and': [
            {'visited': False},
            {'queued': True}]},
        {'$and': [
            {'visited': False},
            {'queued': False},
            {'timeout': {'$exists': False}}]}]})

    return not ((result is None) or (len(result) == 0))


def insert_crawler_start(db):
    """Save when crawler start into database"""
    result = db['CrawlerInfo'].update({'_id': 1},
                                      {'$set':
                                       {'time.start': str(datetime.now())}},
                                      upsert=True)

    return result is not None


def insert_crawler_end(db):
    """Save when crawler ends into database"""
    result = db['CrawlerInfo'].update({'_id': 1},
                                      {'$set':
                                       {'time.end': str(datetime.now())}},
                                      upsert=True)

    return result is not None


def load_seed(seed_path, database):
    """Load urls seed from file"""

    # Load url from file
    seed_urls = urls.load_urls_from_file(seed_path)

    number_of_url = 0

    # Insert loaded urls into database
    for url in seed_urls:
        url = urls.clean(url)
        if validator.validate(url):
            insert_result = db.insert_url(database,
                                          url,
                                          False,
                                          False,
                                          int(CONFIG.get('Settings', 'max_depth')))

            if insert_result:
                number_of_url = number_of_url + 1

    return number_of_url


def main(args=None):
    print("******************************")
    print("UPOL-Crawler v{0}".format(CONFIG.get('Info', 'version')))
    print("******************************")
    print("LOADING..")

    start_load_time = datetime.now()

    # Start procedure
    client = pymongo.MongoClient(
      CONFIG.get('Database', 'db_server'),
      int(CONFIG.get('Database', 'db_port')),
      maxPoolSize=None)
    database = client[DATABASE_NAME]

    # Init database
    db.init(database)

    if load_seed(SEED_FILE, database) == 0:
        print("WARNING: Nothing was added from seed.txt")
    else:
        insert_crawler_start(database)

    end_load_time = datetime.now()

    if CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
        os.makedirs(CPROFILE_DIR, exist_ok=True)
        print("Deleting cprofile folder...")
        # Cleaning cprofile folder
        shutil.rmtree(CPROFILE_DIR)

    print("DONE! {0}".format(end_load_time - start_load_time))
    print("------------------------------")
    print("Start crawling...")
    print("******************************")

    start_time = datetime.now()
    sleeping = False
    number_of_waiting = 0
    number_of_added_links = 0

    while True:
        if sleeping is False:
            batch = db.get_batch_url_for_crawl(database,
                                               int(CONFIG.get('Database',
                                                              'db_batch_size')))

            if batch is not None:
                number_of_added_links = len(batch)
            else:
                number_of_added_links = 0

            if batch is not None:
                hashes = []

                for url in batch:
                    hashes.append(url.get('_id'))

                for url in batch:
                    tasks.crawl_url_task.delay(url.get('url'), url.get('depth'))

                db.set_queued_batch(database, hashes)

            sleeping = True
        else:
            print("------------------------------")
            print("Uptime: {0}".format(datetime.now() - start_time))
            print("Added links: {0}".format(number_of_added_links))
            print("Workers are running - SLEEPING")
            print("------------------------------")

            number_of_added_links = 0

            sleep(int(CONFIG.get('Settings', 'delay_between_feeding')))

            if not should_crawler_wait(database):
                number_of_waiting = number_of_waiting + 1
            else:
                number_of_waiting = 0

            if number_of_waiting >= 2:
                break

            sleeping = False

            print("FEEDING...")

    end_time = datetime.now()
    duration = end_time - start_time
    insert_crawler_end(database)

    print("------------------------------")
    print("Crawl FINISHED")
    print("Duration: {0}".format(duration))
    print("------------------------------")

if __name__ == "__main__":
    main()
