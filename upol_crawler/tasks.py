from __future__ import absolute_import, unicode_literals

import cProfile
import datetime
import hashlib
import os
import time


from celery.utils.log import get_task_logger
from upol_crawler.celery import app

from upol_crawler.settings import *


@app.task(rate_limit=CONFIG.get('Settings', 'crawl_frequency'), queue='crawler', ignore_result=True, task_compression='zlib')
def crawl_url_task(url, depth):
    from upol_crawler.core import crawler

    if CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
        os.makedirs(CPROFILE_DIR, exist_ok=True)

        actual_time = str(datetime.datetime.now()).encode('utf-8')

        cprofile_filename = hashlib.sha1(actual_time).hexdigest()
        cprofile_path = os.path.join(CPROFILE_DIR, cprofile_filename)

        cProfile.runctx('crawler.crawl_url(url, depth)',
                        globals=globals(),
                        locals=locals(),
                        filename=cprofile_path)
    else:
        crawler.crawl_url(url, depth)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_reason_task(url, reason, arg={}):
    from upol_crawler.tools import logger
    logger.log_url_reason(url, reason, arg)
