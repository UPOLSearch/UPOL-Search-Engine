from __future__ import absolute_import, unicode_literals
from .celery import app
from celery.utils.log import get_task_logger
from .crawler import crawl_url

logger = get_task_logger(__name__)


@app.task(rate_limit="5/s", queue='important')
def crawl_url_task(url):
    logger.info(str(url))
    response, status, redirected = crawl_url(url)
    if response is not None:
        logger.info(str(url) + " | " + str(response.status_code) + " | " + str(response.reason) +
                    " | " + str(response.headers['Content-Type']) + " | " + str(status) + " | Redirected: " + str(redirected))
    else:
        logger.info(url + " | " + str(status) + " | Redirected: " + str(redirected))
