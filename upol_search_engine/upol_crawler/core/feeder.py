from time import sleep

from upol_search_engine.db import mongodb
from upol_search_engine.upol_crawler import tasks
from upol_search_engine.upol_crawler.core import validator
from upol_search_engine.utils import urls


def load_seed(seed, database, regex, max_depth, blacklist):
    """Load urls seed from file"""
    number_of_url = 0

    seed_urls = load_seed_from_text(seed)

    # Insert loaded urls into database
    for url in seed_urls:
        url = urls.clean(url)
        if validator.validate(url, regex, blacklist):
            insert_result = mongodb.insert_url(database,
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


def feed_crawler(database, crawler_settings, batch_size):
    batch = mongodb.get_batch_url_for_crawl(database, batch_size)

    if batch is not None:
        number_of_added_links = len(batch)
    else:
        number_of_added_links = 0

    if batch is not None:
        hashes = []

        for url in batch:
            hashes.append(url.get('_id'))

        mongodb.set_queued_batch(database, hashes)

        for url in batch:
            tasks.crawl_url_task.delay(url.get('url'),
                                       url.get('depth'),
                                       crawler_settings)

    return number_of_added_links


def sleep_crawler(database, number_of_waiting, delay_between_feeding):
    sleep(delay_between_feeding)

    if not mongodb.should_crawler_wait(database):
        number_of_waiting = number_of_waiting + 1
    else:
        number_of_waiting = 0

    return number_of_waiting
