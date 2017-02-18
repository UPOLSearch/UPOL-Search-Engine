import re

import pymongo
import requests
from bs4 import BeautifulSoup

from upol_crawler import tasks
from upol_crawler.core import link_extractor, validator, limiter
from upol_crawler.db import db_mongodb as db
from upol_crawler.settings import *
from upol_crawler.tools import logger, robots
from upol_crawler.utils import urls

log = logger.universal_logger('crawler')


def load_seed(seed_path, database):
    """Load urls seed from file"""

    # Load url from file
    seed_urls = urls.load_urls_from_file(seed_path)

    number_of_url = 0

    # Insert loaded urls into database
    for url in seed_urls:
        url = urls.clean(url)
        if validator.validate(url):
            insert_result = db.insert_url(database,
                                          url,
                                          False,
                                          False,
                                          int(CONFIG.get('Settings', 'max_depth')))

            if insert_result:
                number_of_url = number_of_url + 1

    return number_of_url


def request_url(url):
    """Request url and check if content-type is valid"""
    headers = {'user-agent': CONFIG.get('Info', 'user_agent')}
    response = requests.get(url,
                            headers=headers,
                            verify=CONFIG.getboolean('Settings', 'verify_ssl'),
                            timeout=(
                                float(CONFIG.get('Settings', 'connect_max_timeout')),
                                float(CONFIG.get('Settings', 'read_max_timeout'))))

    content_type = response.headers.get('Content-Type')

    if content_type is not None:
        if not validator.validate_content_type(response.headers['Content-Type']):
            return None

    return response


def get_url(url):
    """Return url, original_url and boolean if url was redirected"""
    response = request_url(url)
    redirected = False
    original_url = url

    if response is not None:
        url = urls.clean(response.url)

        if original_url != url:
            redirected = True

    return url, original_url, redirected, response


def crawl_url(url, depth):
    client = pymongo.MongoClient(
      CONFIG.get('Database', 'db_server'),
      int(CONFIG.get('Database', 'db_port')),
      maxPoolSize=None)
    database = client[DATABASE_NAME]

    if not limiter.is_crawl_allowed(url):
        db.set_url_for_recrawl(database, url)
        client.close()
        return

    try:
        url, original_url, redirected, response = get_url(url)
    except requests.exceptions.ReadTimeout as e:
        # It also remove url from queue and set it as timeouted
        db.set_timeout_url(database, url)
        log.warning('(Timeout) - ReadTimeout: {0}'.format(url))
        return
    except requests.exceptions.ConnectionError as e:
        # It also remove url from queue and set it as timeouted
        db.set_timeout_url(database, url)
        log.warning('(Timeout) - ConnectionError: {0}'.format(url))
        return
    except requests.exceptions.ChunkedEncodingError as e:
        # It also remove url from queue and set it as timeouted
        db.set_timeout_url(database, url)
        log.warning('(Timeout) - ChunkedEncodingError: {0}'.format(url))
        return
    except Exception as e:
        db.delete_url(database, url)
        log.exception('Exception: {0}'.format(url))
        raise
    else:
        # Content type is invalid
        if response is None:
            # If original url was redirected delete original url from database
            if redirected:
                db.delete_url(database, original_url)

            db.delete_url(database, url)

            tasks.collect_url_info_task.delay(url, 'UrlIsFile')

            client.close()

            log.info('Content-Type: {0}'.format(url))

            return

        if redirected:
            # Check if redirected url is valid
            valid, reason = validator.validate(url)

            # Delete original url from db, we want to keep only working urls
            db.delete_url(database, original_url)

            if not valid:
                client.close()
                tasks.collect_url_info_task.delay(url,
                                                  'UrlNotValidRedirect',
                                                  {'reason': reason,
                                                   'original_url': original_url})

                log.info('Not Valid Redirect: {0} (original: {1})'.format(url, original_url))

                return

            if not db.exists_url(database, url):
                if urls.is_same_domain(url, original_url):
                    db.insert_url(database, url, True, False, depth - 1)
                else:
                    db.insert_url(database,
                                  url,
                                  True,
                                  False,
                                  int(CONFIG.get('Settings', 'max_depth')))
            else:
                if db.is_visited(database, url):
                    client.close()
                elif db.is_queued(database, url):
                    tasks.collect_url_info_task.delay(url, 'UrlIsAlreadyInQueue')
                    client.close()

                return

        # Begin parse part, should avoid 404
        try:
            html = response.text
            soup = BeautifulSoup(html, 'lxml')
            validated_urls_on_page = link_extractor.validated_page_urls(soup, url)

            urls_for_insert = []

            for page_url in validated_urls_on_page:
                insert_url = {'url': page_url}
                insert_url['url'] = page_url
                if urls.is_same_domain(url, page_url):
                    if depth - 1 != 0:
                        insert_url['depth'] = depth - 1
                    else:
                        continue
                else:
                    insert_url['depth'] = int(CONFIG.get('Settings', 'max_depth'))

                urls_for_insert.append(insert_url)

            if len(urls_for_insert) > 0:
                # Maybe use for-else
                db.batch_insert_url(database, urls_for_insert, False, False)
        except Exception as e:
            db.delete_url(database, url)
            log.exception('Exception: {0}'.format(url))
            raise

        db.set_visited_url(database, url, response, html)

        client.close()
        log.info('Done [{0}]: {1}'.format(response.reason, url))
