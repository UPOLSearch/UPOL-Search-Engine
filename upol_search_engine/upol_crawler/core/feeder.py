from time import sleep

# from upol_search_engine import settings
from upol_search_engine.upol_crawler import db, tasks
# from upol_search_engine.upol_crawler.celery_app import app
from upol_search_engine.upol_crawler.core import validator
# from upol_search_engine.upol_crawler.tools import logger
from upol_search_engine.upol_crawler.utils import urls


# log = logger.universal_logger('feeder')

def load_seed(seed, database, regex, max_depth, blacklist):
    """Load urls seed from file"""
    number_of_url = 0

    seed_urls = load_seed_from_text(seed)

    # Insert loaded urls into database
    for url in seed_urls:
        url = urls.clean(url)
        if validator.validate(url, regex, blacklist):
            insert_result = db.insert_url(database,
                                          url,
                                          False,
                                          False,
                                          max_depth)

            if insert_result:
                number_of_url = number_of_url + 1

    return number_of_url


def load_seed_from_text(seed):
    """Load urls seed from text"""

    seed_urls = urls.load_urls_from_text(seed)

    return seed_urls


def load_seed_from_file(seed_path):
    """Load urls seed from file"""

    seed_urls = urls.load_urls_from_file(seed_path)

    return seed_urls

#
# def start_crawler():
#     client = db.create_client()
#     database = client[settings.DB_NAME]
#
#     # Init database
#     db.init(database)
#
#     return client, database


def feed_crawler(database, crawler_settings, batch_size):
    batch = db.get_batch_url_for_crawl(database, batch_size)

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
            tasks.crawl_url_task.delay(url.get('url'),
                                       url.get('depth'),
                                       crawler_settings)

    return number_of_added_links


def sleep_crawler(database, number_of_waiting, delay_between_feeding):
    sleep(delay_between_feeding)

    if not db.should_crawler_wait(database):
        number_of_waiting = number_of_waiting + 1
    else:
        number_of_waiting = 0

    return number_of_waiting


# def main():
#     import sys
#     try:
#         print("******************************")
#         print("UPOL-Crawler v{0}".format(settings.CONFIG.get('Info', 'version')))
#         print("******************************")
#         print("LOADING..")
#
#         start_load_time = datetime.now()
#
#         # Start procedure
#         client, database = start_crawler()
#
#         if len(sys.argv) > 1:
#             load_seed(sys.argv[1], database)
#         else:
#             insert_crawler_start(database)
#
#         end_load_time = datetime.now()
#
#         if settings.CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
#             os.makedirs(CPROFILE_DIR, exist_ok=True)
#             print("Deleting cprofile folder...")
#             # Cleaning cprofile folder
#             shutil.rmtree(CPROFILE_DIR)
#
#         print("DONE! {0}".format(end_load_time - start_load_time))
#         print("------------------------------")
#         print("Start crawling...")
#         print("******************************")
#
#         start_time = datetime.now()
#         sleeping = False
#         number_of_waiting = 0
#         number_of_added_links = 0
#
#         while True:
#             if sleeping is False:
#                 number_of_added_links = feed_crawler(database)
#
#                 sleeping = True
#             else:
#                 print("------------------------------")
#                 print("Uptime: {0}".format(datetime.now() - start_time))
#                 print("Added links: {0}".format(number_of_added_links))
#                 print("Workers are running - SLEEPING")
#                 print("------------------------------")
#
#                 number_of_added_links = 0
#
#                 number_of_waiting = sleep_crawler(database, number_of_waiting)
#
#                 if number_of_waiting >= 2:
#                     break
#
#                 sleeping = False
#
#                 print("FEEDING...")
#
#         end_time = datetime.now()
#         duration = end_time - start_time
#         insert_crawler_end(database)
#
#         print("------------------------------")
#         print("Crawl FINISHED")
#         print("Duration: {0}".format(duration))
#         print("------------------------------")
#     except Exception as e:
#         log.exception('Exception: {0}'.format(url))
#         raise
