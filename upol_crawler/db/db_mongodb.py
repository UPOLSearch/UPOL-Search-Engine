import random
import urllib.parse
from datetime import datetime
from random import shuffle

import pymongo
from upol_crawler.settings import *
from upol_crawler.utils import urls


def init(db):
    db['Urls'].create_index('visited')
    db['Urls'].create_index('queued')
    db['Urls'].create_index('timeout')
    db['Limiter'].create_index('ip', unique=True)


def _prepare_url_object(url, visited, queued, depth):
    url_object = {'_id': urls.hash(url),
                  'url': url,
                  'domain': urls.domain(url),
                  'depth': depth,
                  'visited': visited,
                  'queued': queued}

    url_object['progress'] = {}
    url_object['progress']['discovered'] = str(datetime.now())

    return url_object


def _universal_insert_url(url, collection, visited, queued, depth):
    url_object = _prepare_url_object(url, visited, queued, depth)

    try:
        result = collection.insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result


def insert_url(db, url, visited, queued, depth):
    """Insert url into db"""
    return _universal_insert_url(url, db['Urls'], visited, queued, depth)


def insert_url_info(db, url, info_type, arg={}):
    collection = db[info_type]

    log_object = {'_id': urls.hash(url),
                  'url': url}

    for key, depth in arg.items():
        log_object[key] = str(depth)

    try:
        collection.insert_one(log_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False


def batch_insert_url(db, urls_with_depths, visited, queued):
    """Inser batch of urls into db"""

    url_documents = []

    for url in urls_with_depths:
        url_object = _prepare_url_object(url.get('url'),
                                         visited,
                                         queued,
                                         url.get('depth'))
        url_documents.append(url_object)

    try:
        result = db['Urls'].insert_many(url_documents, ordered=False)
    except pymongo.errors.BulkWriteError:
        result = None

    return result


def delete_url(db, url):
    """Try to delete url from db, returns True if case of success"""
    result = db['Urls'].delete_one({'_id': urls.hash(url)})

    return result.deleted_count > 0


def exists_url(db, url):
    """Return if url is exists in db"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one({'_id': url_hash})

    return result is not None


def get_batch_url_for_crawl(db, size):
    """Return batch of url from db for crawl"""
    db_batch = list(db['Urls'].aggregate([{'$match':
                                           {'$and': [
                                               {'visited': False},
                                               {'queued': False},
                                               {'timeout': {
                                                   '$exists': False}}]}},
                                          {'$sample': {'size': size}}]))

    if len(db_batch) != 0:
        batch = []

        for field in db_batch:
            url = {'_id': field.get('_id'),
                   'url': field.get('url'),
                   'depth': field.get('depth')}

            batch.append(url)
            shuffle(batch)

        return batch
    else:
        return None


def set_visited_url(db, url, response, html):
    """Try to set url to visited and update other important informations"""
    url_hash = urls.hash(url)

    is_permanent_redirect = False

    for history in response.history:
        if history.is_permanent_redirect:
            is_permanent_redirect = True
            break

    is_redirect = False

    for history in response.history:
        if history.is_permanent_redirect:
            is_redirect = True
            break

    url_addition = {}

    url_addition['visited'] = True
    url_addition['queued'] = False

    url_addition['progress.last_visited'] = str(datetime.now())

    url_addition['content.html'] = html
    url_addition['content.hashes.document'] = urls.hash_document(html)
    url_addition['content.encoding'] = response.encoding
    # Later detect language

    url_addition['response.elapsed'] = str(response.elapsed)
    url_addition['response.is_redirect'] = is_redirect
    url_addition['response.is_permanent_redirect'] = is_permanent_redirect
    url_addition['response.status_code'] = response.status_code
    url_addition['response.reason'] = response.reason

    for key, value in response.headers.items():
        url_addition['response.' + str(key)] = str(value)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    return result is not None


def set_queued_url(db, url):
    """Try to set url to queued"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': {'queued': True}})

    return result is not None


def set_queued_batch(db, list_url_hash):
    """Try to set batch of urls to queued"""

    result = db['Urls'].update_many({'_id': {'$in': list_url_hash}},
                                    {'$set': {'queued': True}})

    return result is not None


def set_url_for_recrawl(db, url):
    """Set url for recrawl later"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': {'queued': False,
                                                      'visited': False}})

    return result is not None


def set_timeout_url(db, url):
    """Try to set url as timouted"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': {
                                                'queued': False,
                                                'timeout.timeout': True,
                                                'timeout.last_timeout': str(datetime.now())
                                                }})

    return result is not None


def is_visited(db, url):
    """Check if url is visited"""
    result = db['Urls'].find_one({'visited': True})

    if result is not None:
        return True


def is_queued(db, url):
    """Check if url is queued"""
    result = db['Urls'].find_one({'queued': True})

    if result is not None:
        return True


def is_visited_or_queued(db, url):
    """Check if url is visited or queued"""
    result = db['Urls'].find_one({'$or': [
                                {'visited': True},
                                {'queued': True}
                              ]})

    if result is not None:
        return True


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
    """Save when crawler start into database"""
    result = db['CrawlerInfo'].update({'_id': 1},
                                      {'$set':
                                       {'time.end': str(datetime.now())}},
                                      upsert=True)

    return result is not None


def insert_limits_for_ip(db, domain, ip, last, max_frequency):
    """Insert limits for specific IP"""
    result = db['Limiter'].insert_one({'ip': ip,
                                       'domain': domain,
                                       'last': str(last),
                                       'max_frequency': max_frequency})

    return result is not None


def get_limits_for_ip(db, ip):
    """Return limits informations for specific IP"""
    result = db['Limiter'].find_one({'ip': ip})

    return result


def set_last_for_ip_limit(db, ip, last):
    """Set the last property for specific IP"""
    result = db['Limiter'].update({'ip': ip},
                                  {'$set':
                                   {'last': str(last)}})

    return result is not None
