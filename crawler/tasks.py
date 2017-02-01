# from __future__ import absolute_import, unicode_literals
import cProfile
import datetime
import hashlib
import os
import time

from celery.utils.log import get_task_logger
from crawler import crawler
from crawler.settings import *

from .celery import app
from .logger import log_url, log_url_reason


@app.task(rate_limit="6/s", queue='crawler', ignore_result=True, task_compression='zlib')
def crawl_url_task(url, value):
    if CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
        os.makedirs(CPROFILE_DIR, exist_ok=True)
        cprofile_filename = hashlib.sha1(str(datetime.datetime.now()).encode('utf-8')).hexdigest()
        cprofile_path = os.path.join(CPROFILE_DIR, cprofile_filename)

        cProfile.runctx('crawler.crawl_url(url, value)', globals=globals(), locals=locals(), filename=cprofile_path)
    else:
        crawler.crawl_url(url, value)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_task(url, response):
    log_url(url, response)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_reason_task(url, reason, arg={}):
    log_url_reason(url, reason, arg)
