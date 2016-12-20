from reppy.robots import Robots
from reppy.cache import RobotsCache
import threading
from crawler import config

lock = threading.RLock()

cache = RobotsCache(capacity=500)
headers = {'user-agent': config.user_agent}


def is_crawler_allowed(url):
    allowed = True
    try:
        robots_url = Robots.robots_url(url)
        lock.acquire()
        allowed = cache.allowed(url, config.user_agent)
    except:
        pass
    finally:
        lock.release()

    return allowed
