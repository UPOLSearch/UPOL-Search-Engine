import redis
from . import url

db = redis.StrictRedis(host='localhost', port=6379, db=1)
db_visited = redis.StrictRedis(host='localhost', port=6379, db=2)

def insert_url(url):
    """Insert url into db and set visited False and inlinks 0"""
    args = {'url': url.encode('utf-8')}

    return db.hmset(url, args)

# def get_url(url):
#     """Return url from db"""
#     result = db.hmget(url, 'url')
#
#     if result[0] is None:
#         return None
#
#     return result[0].decode('utf-8')

def delete_url(url):
    """Try to delete url from db, returns True if case of success"""
    result = db.delete(url)

    return result == 1

def exists_url(url):
    """Return if url is exists in db"""
    return (db.exists(url) > 0) or (db_visited.exists(url) > 0)

def set_visited_url(url):
    """Try to set url to visited"""
    if delete_url(url) is True:
        db_visited.hmset(url, {'url': url})
        return True
    else:
        return False

def flush_db():
    """Delete everything from database"""
    return db.flushall(), db_visited.flushall()
