import threading

from reppy.cache import RobotsCache
from upol_search_engine import settings

lock = threading.RLock()

cache = RobotsCache(capacity=500)


def is_crawler_allowed(url):
    allowed = True

    lock.acquire()
    allowed = cache.allowed(url,
                            settings.user_agent)
    lock.release()

    return allowed
