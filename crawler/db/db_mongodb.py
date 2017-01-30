import random
import urllib.parse

import pymongo

from crawler.urls import url_tools


# Global database connection
# client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
# database = client.upol_crawler


def init(db):
    db.urls.create_index('visited')


def _universal_insert_url(url, collection, visited, value):
    url_object = {"_id": url_tools.hash(url),
                  "url": url,
                  "visited": visited,
                  "value": value}
    try:
        result = collection.insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result


def insert_url(db, url, visited, value):
    """Insert url into db"""
    return _universal_insert_url(url, db.urls, visited, value)


def insert_url_visited_file_extension(db, url):
    """Insert url into db as visited"""
    return _universal_insert_url(url, db.urls_file, True, -1)


def delete_url(db, url):
    """Try to delete url from db, returns True if case of success"""
    result = db.urls.delete_one({'_id': url_tools.hash(url)})

    return result.deleted_count > 0


def is_visited(db, url):
    result = db.urls_visited.find_one({"_id": url_tools.hash(url)})

    return result is not None


def exists_url(db, url):
    """Return if url is exists in db"""
    url_hash = url_tools.hash(url)

    result = db.urls.find_one({"_id": url_hash})

    return result is not None


def get_unvisited_url(db):
    """Return unvisited url from db"""
    result = db.urls.find_one({'visited': False})

    if result is not None:
        return result['url'], result['value']
    else:
        return None, None


def get_random_unvisited_url(db):
    """Return random unvisited url"""
    result = list(db.urls.aggregate([{"$match": {'visited': False}}, {"$sample": {'size': 1}}]))
    if len(result) != 0:
        return result[0]['url'], result[0]['value']
    else:
        return None, None


def set_visited_url(db, url):
    """Try to set url to visited"""
    url_hash = url_tools.hash(url)

    result = db.urls.find_one_and_update({"_id": url_hash}, {'$set': {'visited': True}})

    return result is not None


def flush_db():
    """Delete everything from database"""
    return db.urls.drop()
