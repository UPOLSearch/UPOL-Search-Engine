
import requests
from bs4 import BeautifulSoup
from celery.utils.log import get_task_logger
from upol_search_engine import settings
from upol_search_engine.db import mongodb
from upol_search_engine.upol_crawler.core import (limiter, link_extractor,
                                                  validator)
from upol_search_engine.upol_crawler.tools import blacklist
from upol_search_engine.utils import urls

log = get_task_logger(__name__)


def get_response(url, connect_max_timeout, read_max_timeout):
    """Request url and check if content-type is valid"""
    headers = {'user-agent': settings.user_agent}
    response = requests.get(url,
                            headers=headers,
                            verify=False,
                            timeout=(
                                connect_max_timeout,
                                read_max_timeout))

    return response


def get_page(url, connect_max_timeout, read_max_timeout):
    """Return url, original_url and boolean if url was redirected"""
    response = get_response(url, connect_max_timeout, read_max_timeout)
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


def test_content_type_file(content_type):
    return 'text/html' not in content_type


def test_file_valid_type(content_type):
    if (('application/pdf' not in content_type) and ('text/plain' not in content_type)):
        return False
    else:
        return True


def _handle_response(database, url, original_url, redirected,
                     response, depth, max_depth, limit_domain, blacklist,
                     ignore_blacklist=False):
    try:
        url_document = mongodb.get_url(database, url)
        regex = urls.generate_regex(limit_domain)

        # Redirect handling
        if original_url != url:
            log.info('Redirect: {1} (original: {0})'.format(original_url, url))

            # Check if redirected url is valid
            is_valid_redirect, reason = validator.validate(url, regex,
                                                           blacklist)

            if (is_valid_redirect is False) and (reason == 'UrlIsBlacklisted') and ignore_blacklist:
                is_valid_redirect = True

            if is_valid_redirect:
                mongodb.set_alias_visited_url(database, original_url)

                url_document = mongodb.get_url(database, url)

                if url_document is not None:
                    if url_document.get('visited') and not url_document.get('alias'):
                        canonical_group = url_document.get('canonical_group')
                        mongodb.set_canonical_group_to_alias(database, original_url,
                                                             canonical_group)

                        log.info('Already visited redirect: {0} (original: {1})'.format(
                            url, original_url))

                        return
                else:
                    if not urls.is_same_domain(url, original_url):
                        depth = max_depth

                    mongodb.insert_url(database, url, False, False, depth)

            else:
                mongodb.set_visited_invalid_url(database, original_url,
                                                response, "invalid_redirect")
                mongodb.delete_pagerank_edge_to(database, urls.hash(original_url))

                log.info('Not Valid Redirect: {0} (original: {1})'.format(
                    url, original_url))

                return
        else:
            # Check if url is already visited
            if url_document is not None:
                if url_document.get('visited'):
                    log.info('Already visited: {0}'.format(url))
                    return

        # File handling
        content_type = response.headers.get('Content-Type')

        if content_type is None:
            content_type = ''

        is_content_type_file = test_content_type_file(content_type)
        is_file_valid_type = test_file_valid_type(content_type)

        if is_content_type_file:
            if not is_file_valid_type:
                mongodb.delete_pagerank_edge_to(database, urls.hash(url))
                mongodb.set_visited_invalid_url(database, url, response,
                                                "invalid_file", True)

                log.info('Not valid file: {0}'.format(url))
                return
            else:
                if original_url != url:
                    mongodb.set_visited_file_url(database, url,
                                                 response, original_url)
                else:
                    mongodb.set_visited_file_url(database, url, response)
                log.info('Done (file) [{0}]: {1}'.format(response.reason, url))
        else:
            # Handle normal page
            soup = BeautifulSoup(response.content, 'html5lib')
            no_index = link_extractor.has_noindex(soup)
            validated_urls_on_page, not_valid_urls = link_extractor.validated_page_urls(
                soup, url, regex, blacklist)

            urls_for_insert = []

            for page_url in validated_urls_on_page:
                insert_url = {'url': page_url}

                if urls.is_same_domain(url, page_url):
                    if depth - 1 != 0:
                        insert_url['depth'] = depth - 1
                    else:
                        continue
                else:
                    insert_url['depth'] = max_depth

                urls_for_insert.append(insert_url)

            if len(urls_for_insert) > 0:
                mongodb.batch_insert_url(database, urls_for_insert, False, False)
                mongodb.batch_insert_pagerank_outlinks(database, url,
                                                       urls_for_insert)

            if original_url != url:
                mongodb.set_visited_url(database, url, response, soup,
                                        no_index, original_url)
            else:
                mongodb.set_visited_url(database, url, response, soup, no_index)

            log.info('Done [{0}]: {1}'.format(response.reason, url))

            return
    except Exception as e:
        mongodb.delete_url(database, url)
        log.exception('Exception: {0} {1}'.format(url, e))
        raise


def crawl_url(url, depth, crawler_settings, ignore_blacklist=False):
    try:
        client = mongodb.create_client()
        database = mongodb.get_database(crawler_settings.get('limit_domain'),
                                        client)

        allowed = limiter.is_crawl_allowed(url, database, crawler_settings.get(
            'frequency_per_server'))

        if not allowed:
            mongodb.set_url_for_recrawl(database, url)
            client.close()
            return

        url, original_url, redirected, response = get_page(
            url, crawler_settings.get('connect_max_timeout'),
            crawler_settings.get('read_max_timeout'))
    except requests.exceptions.ReadTimeout as e:
        # It also remove url from queue and set it as timeouted
        mongodb.set_timeout_url(database, url)
        log.warning('(Timeout) - ReadTimeout: {0}'.format(url))
    except requests.exceptions.ConnectionError as e:
        # It also remove url from queue and set it as timeouted
        mongodb.set_timeout_url(database, url)
        log.warning('(Timeout) - ConnectionError: {0}'.format(url))
    except requests.exceptions.ChunkedEncodingError as e:
        # It also remove url from queue and set it as timeouted
        mongodb.set_timeout_url(database, url)
        log.warning('(Timeout) - ChunkedEncodingError: {0}'.format(url))
    except Exception as e:
        mongodb.delete_url(database, url)
        log.exception('Exception: {0}'.format(url))
        client.close()
        raise
    else:
        _handle_response(database,
                         url,
                         original_url,
                         redirected,
                         response,
                         depth,
                         crawler_settings.get('max_depth'),
                         crawler_settings.get('limit_domain'),
                         crawler_settings.get('blacklist'),
                         ignore_blacklist)

    client.close()
