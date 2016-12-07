import pymongo
import urllib.parse
from crawler import urls

client = pymongo.MongoClient('localhost', 27017)
db = client.upol_crawler

def init():
    None
    # db.urls.create_index('url_hash', unique=True)
    # db.urls_visited.create_index('url_hash', unique=True)

def _universal_insert_url(url, collection):
    url_object = {"_id": urls.hash(url),
                  "url": url}
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
    result = db.urls.delete_one({'_id': urls.hash(url)})

    return result.deleted_count > 0

def exists_url(url):
    """Return if url is exists in db"""
    url_hash = urls.hash(url)

    result = db.urls.find({"_id": url_hash}).limit(1)
    result_visited = db.urls_visited.find({"_id": url_hash}).limit(1)

    return result.count() + result_visited.count() > 0

def random_unvisited_url():
    """Return random unvisited url"""
    return list(db.urls.aggregate([{"$sample": {'size': 1}}]))[0]['url']

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
