import re

import pymongo
import requests
from bs4 import BeautifulSoup
from upol_crawler import db, tasks
from upol_crawler.core import limiter, link_extractor, validator
from upol_crawler.settings import *
from upol_crawler.tools import logger, robots
from upol_crawler.utils import urls

log = logger.universal_logger('crawler')


def get_response(url):
    """Request url and check if content-type is valid"""
    headers = {'user-agent': CONFIG.get('Info', 'user_agent')}
    response = requests.get(url,
                            headers=headers,
                            verify=CONFIG.getboolean('Settings', 'verify_ssl'),
                            timeout=(
                                float(CONFIG.get('Settings', 'connect_max_timeout')),
                                float(CONFIG.get('Settings', 'read_max_timeout'))))

    # content_type = response.headers.get('Content-Type')
    #
    # if content_type is not None:
    #     if not validator.validate_content_type(response.headers['Content-Type']):
    #         return None

    return response


def get_page(url):
    """Return url, original_url and boolean if url was redirected"""
    response = get_response(url)
    original_url = url

    is_redirect = False

    if response is not None:
        # Detect if response is redirect
        for history in response.history:
            if history.is_redirect:
                is_redirect = True
                break

        url = urls.clean(response.url)

    return url, original_url, is_redirect, response


def _handle_response(database, url, original_url, redirected, response, depth):
    try:
        # Redirect handling
        if (redirected and original_url != url):
            log.info('Redirect: {0} (original: {1})'.format(original_url, url))

            # Check if redirected url is valid
            is_valid_redirect, reason = validator.validate(url)

            if is_valid_redirect:
                db.set_alias_visited_url(database, original_url)

                if not db.exists_url(database, url):
                    if not urls.is_same_domain(url, original_url):
                        depth = int(CONFIG.get('Settings', 'max_depth'))

                    db.insert_url(database, url, False, False, depth)
                # else:
                #     if db.is_queued(database, url):
                #         log.info('Already queued: {0}'.format(url))
                #         return
                #     else:
                #         # Set the url as queued, so the feeder know the url is in the progress
                #         db.set_queued_url(database, url)
            else:
                db.set_visited_invalid_url(database, original_url, response, "invalid_redirect")
                db.delete_pagerank_edge_to(database, urls.hash(original_url))

                log.info('Not Valid Redirect: {0} (original: {1})'.format(url, original_url))

                return

        # File handling
        content_type = response.headers.get('Content-Type')

        if content_type is None:
            content_type = ''

        if 'text/html' not in content_type:
            # 'application/msword' in content_type or
            # 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type
            if (('application/pdf' not in content_type) and ('text/plain' not in content_type)):
                # Not valid file
                if content_type is not '':
                    content_type = content_type.split(';')[0]
                else:
                    content_type = 'unknown'

                db.delete_pagerank_edge_to(database, urls.hash(url))
                db.set_visited_invalid_url(database, url, response, "invalid_file")

                log.info('Not valid file: {0}'.format(url))
                return
            else:
                # Handle file
                if (redirected and original_url != url):
                    db.set_visited_file_url(database, url, response, original_url)
                else:
                    db.set_visited_file_url(database, url, response)
                log.info('Done (file) [{0}]: {1}'.format(response.reason, url))
        else:
            # Handle normal page
            soup = BeautifulSoup(response.content, 'lxml')
            no_index = link_extractor.has_noindex(soup)
            validated_urls_on_page = link_extractor.validated_page_urls(soup, url)

            urls_for_insert = []

            for page_url in validated_urls_on_page:
                insert_url = {'url': page_url}

                if urls.is_same_domain(url, page_url):
                    if depth - 1 != 0:
                        insert_url['depth'] = depth - 1
                    else:
                        continue
                else:
                    insert_url['depth'] = int(CONFIG.get('Settings', 'max_depth'))

                urls_for_insert.append(insert_url)

            if len(urls_for_insert) > 0:
                db.batch_insert_url(database, urls_for_insert, False, False)
                db.batch_insert_pagerank_outlinks(database, url, urls_for_insert)

            db.set_visited_url(database, url, response, soup, no_index, original_url)
            log.info('Done [{0}]: {1}'.format(response.reason, url))

    except Exception as e:
        db.delete_url(database, url)
        log.exception('Exception: {0}'.format(url))
        raise


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
        url, original_url, redirected, response = get_page(url)
    except requests.exceptions.ReadTimeout as e:
        # It also remove url from queue and set it as timeouted
        db.set_timeout_url(database, url)
        log.warning('(Timeout) - ReadTimeout: {0}'.format(url))
    except requests.exceptions.ConnectionError as e:
        # It also remove url from queue and set it as timeouted
        db.set_timeout_url(database, url)
        log.warning('(Timeout) - ConnectionError: {0}'.format(url))
    except requests.exceptions.ChunkedEncodingError as e:
        # It also remove url from queue and set it as timeouted
        db.set_timeout_url(database, url)
        log.warning('(Timeout) - ChunkedEncodingError: {0}'.format(url))
    except Exception as e:
        db.delete_url(database, url)
        log.exception('Exception: {0}'.format(url))
        client.close()
        raise
    else:
        _handle_response(database, url, original_url, redirected, response, depth)

    client.close()
