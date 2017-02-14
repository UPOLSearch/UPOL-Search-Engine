import socket
import threading
import pymongo
from datetime import datetime

from upol_crawler.utils import urls
from upol_crawler.db import db_mongodb as db
from upol_crawler.tools import logger
from upol_crawler.settings import *


log = logger.universal_logger('limiter')


def _time_difference_from_now(time):
    """Return time difference from time to now"""
    delta = datetime.now() - time
    return delta.total_seconds()


def get_ip(url):
    """Return IP of website from URL"""
    domain = urls.domain(url)
    return socket.gethostbyname(domain)


def is_crawl_allowed(url):
    """Check if crawler is allowed to crawl given URL"""
    client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
    database = client[DATABASE_NAME]

    ip = get_ip(url)

    result = True
    record = db.get_limits_for_ip(database, ip)

    if record is not None:
        last = datetime.strptime(record['last'], '%Y-%m-%d %H:%M:%S.%f')
        delta = _time_difference_from_now(last)

        if delta < float(record['max_frequency']):
            result = False
        else:
            db.set_last_for_ip_limit(database, ip, datetime.now())

    else:
        db.insert_limits_for_ip(database,
                                urls.domain(url),
                                ip,
                                datetime.now(),
                                float(CONFIG.get('Settings', 'crawl_frequency_per_server')))

    if not result:
        log.info('Limited: {0}'.format(url))

    return result
