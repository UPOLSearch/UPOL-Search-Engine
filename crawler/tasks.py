# from __future__ import absolute_import, unicode_literals
from .celery import app
from celery.utils.log import get_task_logger
from crawler import crawler
from .logger import log_url
from .logger import log_url_validator
from .logger import log_url_reason

logger = get_task_logger(__name__)


@app.task(rate_limit="6/s", queue='crawler', ignore_result=True, task_compression='zlib')
def crawl_url_task(url, value):
    # crawl_url(url, value)
    crawler.crawl_url(url, value)
    # response, status, redirected = crawler.crawl_url(url, value)
    # if response is not None:
    #     logger.info(str(url) + " | " + str(response.status_code) + " | " + str(response.reason) +
    #                 " | " + str(response.headers['Content-Type']) + " | " + str(status) + " | Redirected: " + str(redirected))
    # else:
    #     logger.info(url + " | " + str(status) + " | Redirected: " + str(redirected))


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_task(url, response):
    log_url(url, response)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_validator_task(url, validator, arg=None):
    log_url_validator(url, validator, arg)


@app.task(queue='logger', ignore_result=True, task_compression='zlib')
def log_url_reason_task(url, reason, arg={}):
    log_url_reason(url, validator, arg)
