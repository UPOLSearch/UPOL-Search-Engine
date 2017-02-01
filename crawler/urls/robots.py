import threading

from crawler.settings import *
from reppy.cache import RobotsCache
from reppy.robots import Robots

lock = threading.RLock()

cache = RobotsCache(capacity=500)
headers = {'user-agent': CONFIG.get('Info', 'user_agent')}


def is_crawler_allowed(url):
    allowed = True

    lock.acquire()
    robots_url = Robots.robots_url(url)
    allowed = cache.allowed(url, CONFIG.get('Info', 'user_agent'))
    lock.release()

    return allowed
