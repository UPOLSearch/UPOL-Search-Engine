# from __future__ import absolute_import, unicode_literals
from celery.utils.log import get_task_logger

from crawler import crawler

from .celery import app
from .logger import log_url, log_url_reason, log_url_validator


@app.task(rate_limit="6/s", queue='crawler', ignore_result=True, task_compression='zlib')
def crawl_url_task(url, value):
    crawler.crawl_url(url, value)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_task(url, response):
    log_url(url, response)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_reason_task(url, reason, arg={}):
    log_url_reason(url, reason, arg)
