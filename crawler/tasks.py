# from __future__ import absolute_import, unicode_literals
from .celery import app
from celery.utils.log import get_task_logger
from crawler import crawler
from .logger import log_url
from .logger import log_url_validator

logger = get_task_logger(__name__)


@app.task(rate_limit="6/s", queue='crawler', ignore_result=True)
def crawl_url_task(url, value):
    # crawl_url(url, value)
    response, status, redirected = crawler.crawl_url(url, value)
    # if response is not None:
    #     logger.info(str(url) + " | " + str(response.status_code) + " | " + str(response.reason) +
    #                 " | " + str(response.headers['Content-Type']) + " | " + str(status) + " | Redirected: " + str(redirected))
    # else:
    #     logger.info(url + " | " + str(status) + " | Redirected: " + str(redirected))


# @app.task(queue='logger', ignore_result=True)
# def log_url_task(url, response):
#     log_url(url, response)
#
#
# @app.task(queue='logger', ignore_result=True)
# def log_url_validator_task(url, validator):
#     log_url_validator(url, validator)
