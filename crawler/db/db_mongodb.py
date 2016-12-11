import pymongo
import urllib.parse
import random
from crawler.urls import url_tools


client = pymongo.MongoClient('localhost', 27017)
db = client.upol_crawler


def init():
    None
    # db.urls.create_index('url_hash', unique=True)
    # db.urls_visited.create_index('url_hash', unique=True)


def _universal_insert_url(url, collection):
    url_object = {"_id": url_tools.hash(url),
                  "url": url,
                  "random": random.random()}
    try:
        result = collection.insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result


def insert_url(url):
    """Insert url into db and set visited False and inlinks 0"""
    return _universal_insert_url(url, db.urls)


def delete_url(url):
    """Try to delete url from db, returns True if case of success"""
    result = db.urls.delete_one({'_id': url_tools.hash(url)})

    return result.deleted_count > 0


def is_visited(url):
    result_visited = db.urls_visited.find({"_id": url_tools.hash(url)}).limit(1)

    return result_visited.count() > 0


def exists_url(url):
    """Return if url is exists in db"""
    url_hash = url_tools.hash(url)

    result = db.urls.find({"_id": url_hash}).limit(1)
    result_visited = db.urls_visited.find({"_id": url_hash}).limit(1)

    return result.count() + result_visited.count() > 0

def number_of_unvisited_url():
    """Return number of unvisited url"""
    return db.urls.count()

def random_unvisited_url_deprecated():
    """Return random unvisited url"""
    rand = random.random()
    random_record = db.urls.find_one({ "random": { "gte": rand }})

    if random_record is not None:
        return random_record['url']
    else:
        return None

def random_unvisited_url():
    """Return random unvisited url"""
    if number_of_unvisited_url() > 0:
        result = list(db.urls.aggregate([{"$sample": {'size': 1}}]))
        while len(result) > 0:
            result = list(db.urls.aggregate([{"$sample": {'size': 1}}]))
        return result[0]['url']
    else:
        return None

def set_visited_url(url):
    """Try to set url to visited"""
    if (delete_url(url)):
        _universal_insert_url(url, db.urls_visited)
        return True
    else:
        return False


def flush_db():
    """Delete everything from database"""
    return db.urls.drop(), db.urls_visited.drop()
