import random
import urllib.parse

import pymongo

from crawler.urls import url_tools


# Global database connection
# client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
# database = client.upol_crawler


def init(db):
    db.urls.create_index('visited')
    db.urls.create_index('queued')


def _universal_insert_url(url, collection, visited, queued, value):
    url_object = {"_id": url_tools.hash(url),
                  "url": url,
                  "visited": visited,
                  "queued": queued,
                  "value": value}
    try:
        result = collection.insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result


def insert_url(db, url, visited, queued, value):
    """Insert url into db"""
    return _universal_insert_url(url, db.urls, visited, queued, value)


def delete_url(db, url):
    """Try to delete url from db, returns True if case of success"""
    result = db.urls.delete_one({'_id': url_tools.hash(url)})

    return result.deleted_count > 0


def exists_url(db, url):
    """Return if url is exists in db"""
    url_hash = url_tools.hash(url)

    result = db.urls.find_one({"_id": url_hash})

    return result is not None


# def get_unvisited_url(db):
#     """Return unvisited url from db"""
#     result = db.urls.find_one({'visited': False})
#
#     if result is not None:
#         return result['url'], result['value']
#     else:
#         return None, None
#
#
# def get_random_unvisited_url(db):
#     """Return random unvisited url"""
#     result = list(db.urls.aggregate([{"$match": {'visited': False}}, {"$sample": {'size': 1}}]))
#     if len(result) != 0:
#         return result[0]['url'], result[0]['value']
#     else:
#         return None, None


def get_url_for_crawl(db):
    """Return url from db which is ready for crawling - unvisited and unqueued"""
    result = db.urls.find_one({"$and": [
                                {'visited': False},
                                {'queued': False}
                              ]})

    if result is not None:
        return result['url'], result['value']
    else:
        return None, None


def get_random_url_for_crawl(db):
    """Return random url from db which is ready for crawling - unvisited and unqueued"""
    result = list(db.urls.aggregate([{"$match":
                                      {"$and": [
                                          {'visited': False},
                                          {'queued': False}
                                      ]}}, {"$sample": {'size': 1}}]))

    if len(result) != 0:
        return result[0]['url'], result[0]['value']
    else:
        return None, None


def set_visited_url(db, url):
    """Try to set url to visited"""
    url_hash = url_tools.hash(url)

    result = db.urls.find_one_and_update({"_id": url_hash}, {'$set': {'visited': True, 'queued': False}})

    return result is not None


def set_queued_url(db, url):
    """Try to set url to queued"""
    url_hash = url_tools.hash(url)

    result = db.urls.find_one_and_update({"_id": url_hash}, {'$set': {'queued': True}})

    return result is not None


def is_visited_or_queued(db, url):
    """Check if url is visited"""
    result = db.urls.find_one({"$or": [
                                {'visited': True},
                                {'queued': True}
                              ]})

    if result is not None:
        return True


def flush_db():
    """Delete everything from database"""
    return db.urls.drop()
