import pymongo
import urllib.parse
from crawler import urls

client = pymongo.MongoClient('localhost', 27017)
db = client.upol_crawler

# db.collection.aggregate([
#     { $sample: {size: 20} },
#     { $match:{"yourField": value} }
#   ])

# result = mongo.db.xxx.delete_one({'_id': ObjectId(_id)})

def init():
    db.urls.create_index('url_hash', unique=True)
    db.urls_visited.create_index('url_hash', unique=True)

def insert_url(url):
    """Insert url into db and set visited False and inlinks 0"""
    url_object = {"_id": urls.hash(url),
                  "url": url,
                  "visited": False}
    try:
        result = db.urls.insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result

def delete_url(url):
    """Try to delete url from db, returns True if case of success"""
    result = db.urls.delete_one({'_id': urls.hash(url)})

    return result.deleted_count > 0

def exists_url(url):
    """Return if url is exists in db"""
    result = db.urls.find({"_id": urls.hash(url)}).limit(1)
    return result.count() > 0

def random_unvisited_url():
    """Return random unvisited url"""

# db.ProductData.update_one({
#   '_id': p['_id']
# },{
#   '$inc': {
#     'd.a': 1
#   }
# }, upsert=False)

def set_visited_url(url):
    """Try to set url to visited"""

def flush_db():
    """Delete everything from database"""
    return db.urls.drop()
