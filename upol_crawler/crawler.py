import re

import pymongo
import requests
from bs4 import BeautifulSoup
from upol_crawler import logger, tasks
from upol_crawler.db import db_mongodb as db
from upol_crawler.settings import *
from upol_crawler.urls import parser, robots, url_tools, validator


def load_seed(seed_path, database):
    """Load urls seed from file"""
    urls = set()

    # Load url from file
    with open(seed_path) as seed_file:
        for line in seed_file:
            # Ignore all white characters
            url = line.rstrip()
            # Take url only if is not commented
            if not line.startswith("#"):
                url = url_tools.clean(url)
                urls.add(url)

    number_of_url = 0

    # Insert loaded urls into database
    for url in urls:
        url = url_tools.clean(url)
        if validator.validate(url):
            if db.insert_url(database, url, False, False, int(CONFIG.get('Settings', 'max_depth'))):
                number_of_url = number_of_url + 1

    return number_of_url


def request_url(url):
    """Request url and check if content-type is valid"""
    headers = {'user-agent': CONFIG.get('Info', 'user_agent')}
    response = requests.get(url, headers=headers, verify=CONFIG.getboolean('Settings', 'verify_ssl'), timeout=int(CONFIG.get('Settings', 'max_timeout')))

    if validator.validate_content_type(response.headers['Content-Type']):
        return response
    else:
        return None


def get_url(url):
    """Return url, original_url and boolean if url was redirected"""
    response = request_url(url)
    redirected = False
    original_url = url

    if response is not None:
        url = url_tools.clean(response.url)

        if original_url != url:
            redirected = True

    return url, original_url, redirected, response


def crawl_url(url, depth):
    client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
    database = client[DATABASE_NAME]

    try:
        url, original_url, redirected, response = get_url(url)
    except requests.exceptions.ReadTimeout:
        # TIMEOUT
        db.set_timeout_url(database, url)
        return None, 'Timeout', None
    except Exception as e:
        db.delete_url(database, url)
        tasks.log_url_reason_task.delay(url, 'UrlException', {'place': 'get_url', 'info': str(e)})
        raise
    else:
        # Content type is invalid
        if response is None:
            # If original url was redirected delete original url from database
            if redirected:
                db.delete_url(database, original_url)

            db.delete_url(database, url)

            tasks.log_url_reason_task.delay(url, 'UrlIsFile')
            
            client.close()

            return response, 'Response is', redirected

        if redirected:
            # Check if redirected url is valid
            valid, reason = validator.validate(url)

            # Delete original url from db, we want to keep only working urls
            db.delete_url(database, original_url)

            if not valid:
                client.close()
                tasks.log_url_reason_task.delay(url, 'UrlNotValidRedirect', {'reason': reason, 'original_url': original_url})
                return response, 'URL is not valid', redirected

            if not db.exists_url(database, url):
                if url_tools.is_same_domain(url, original_url):
                    db.insert_url(database, url, True, False, depth - 1)
                else:
                    db.insert_url(database, url, True, False, int(CONFIG.get('Settings', 'max_depth')))
            else:
                if db.is_visited_or_queued(database, url):
                    client.close()
                    # tasks.log_url_task.delay(url, logger.get_log_format(response))
                    return response, 'URL is already visited', redirected

        # Begin parse part, should avoid 404
        try:
            html = response.text
            soup = BeautifulSoup(html, 'lxml')

            validated_urls_on_page = parser.validated_page_urls(soup, url)

            # for page_url in validated_urls_on_page:
            #     if url_tools.is_same_domain(url, page_url):
            #         if depth - 1 != 0:
            #             db.insert_url(database, page_url, False, False, depth - 1)
            #         else:
            #             tasks.log_url_reason_task.delay(url, 'UrlDepthLimit')
            #     else:
            #         db.insert_url(database, page_url, False, False, int(CONFIG.get('Settings', 'max_depth')))

            urls_for_insert = []

            for page_url in validated_urls_on_page:
                insert_url = {}
                insert_url['url'] = page_url
                if url_tools.is_same_domain(url, page_url):
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
            tasks.log_url_reason_task.delay(url, 'UrlException', {'place': 'parser', 'info': str(e)})
            raise

        db.set_visited_url(database, url, response, html)
        client.close()
        return response, 'URL done', redirected
