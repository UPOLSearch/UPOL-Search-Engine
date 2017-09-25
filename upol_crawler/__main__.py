import shutil
import sys
from datetime import datetime
from time import sleep

import pymongo
from upol_crawler import db, settings, tasks
from upol_crawler.celery_app import app
from upol_crawler.core import crawler, validator
from upol_crawler.tools import logger
from upol_crawler.utils import urls

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


def get_crawler_stats(db):
    stats = {}

    stats['urls_count'] = db['Urls'].count()
    stats['files_count'] = db['Urls'].find({'file': True}).count()
    stats['ignored_files_count'] = db['Urls'].find({'invalid_reason': 'invalid_file', 'invalid': True}).count()
    stats['timeout_count'] = db['Urls'].find({'timeout.timeout': True}).count()
    stats['robots_blocked_count'] = db['UrlRobotsBlocked'].count()
    stats['urls_visited'] = db['Urls'].find({'visited': True}).count()
    stats['urls_queued'] = db['Urls'].find({'$and': [{'visited': False}, {'queued': True}]}).count()
    stats['urls_not_queued'] = db['Urls'].find({'$and': [{'visited': False}, {'queued': False}, {'timeout': {'$exists': False}}]}).count()

    return stats

def load_seed(seed, database):
    """Load urls seed from file"""

    if '.txt' in seed:
        seed_urls = load_seed_from_file(seed)
    else:
        seed_urls = load_seed_from_text(seed)

    number_of_url = 0

    # Insert loaded urls into database
    for url in seed_urls:
        url = urls.clean(url)
        if validator.validate(url):
            insert_result = db.insert_url(database,
                                          url,
                                          False,
                                          False,
                                          settings.MAX_DEPTH)

            if insert_result:
                number_of_url = number_of_url + 1

    if number_of_url == 0:
        print("WARNING: Nothing was added from seed.txt")
    else:
        insert_crawler_start(database)


def load_seed_from_text(seed):
    """Load urls seed from text"""

    if seed is None:
        return []

    seed_urls = urls.load_urls_from_text(seed)

    return seed_urls


def load_seed_from_file(seed_path):
    """Load urls seed from file"""

    # Load url from file
    seed_urls = urls.load_urls_from_file(seed_path)

    return seed_urls

def start_crawler():
    client = pymongo.MongoClient(
      settings.DB_SERVER,
      settings.DB_PORT,
      maxPoolSize=None)
    database = client[settings.DB_NAME]

    # Init database
    db.init(database)

    return client, database


def feed_crawler(database):
    batch = db.get_batch_url_for_crawl(database, settings.DB_BATCH_SIZE)

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

    return number_of_added_links


def sleep_crawler(database, number_of_waiting):
    sleep(settings.DELAY_BETWEEN_FEEDING)

    if not should_crawler_wait(database):
        number_of_waiting = number_of_waiting + 1
    else:
        number_of_waiting = 0

    return number_of_waiting


def main():
    import sys
    try:
        print("******************************")
        print("UPOL-Crawler v{0}".format(settings.CONFIG.get('Info', 'version')))
        print("******************************")
        print("LOADING..")

        start_load_time = datetime.now()

        # Start procedure
        client, database = start_crawler()

        if len(sys.argv) > 1:
            load_seed(sys.argv[1], database)
        else:
            insert_crawler_start(database)

        end_load_time = datetime.now()

        if settings.CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
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
                number_of_added_links = feed_crawler(database)

                sleeping = True
            else:
                print("------------------------------")
                print("Uptime: {0}".format(datetime.now() - start_time))
                print("Added links: {0}".format(number_of_added_links))
                print("Workers are running - SLEEPING")
                print("------------------------------")

                number_of_added_links = 0

                number_of_waiting = sleep_crawler(database, number_of_waiting)

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
    except Exception as e:
        log.exception('Exception: {0}'.format(url))
        raise

if __name__ == "__main__":
    main()
