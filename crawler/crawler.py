import requests
import pymongo
from bs4 import BeautifulSoup
from crawler import config
from crawler.urls import validator
from crawler.urls import parser
from crawler.urls import url_tools
from crawler.urls import robots
from crawler.db import db_mongodb as db


def request_url(url):
    """Request url and check if content-type is valid"""
    headers = {'user-agent': config.user_agent}
    response = requests.head(url, headers=headers, verify=config.verify_ssl)

    if validator.validate_content_type(response.headers['Content-Type']):
        return requests.get(url, headers=headers, verify=config.verify_ssl)
    else:
        return None


def get_url(url):
    """Return url, original_url and boolean if url was redirected"""
    response = request_url(url)
    redirected = False
    original_url = url

    if response is not None:
        # if len(response.history) > 0:
        #     redirected = True
        #     original_url = url
        #     url = url_tools.clean(response.history[0].url)
        #
        #     if url == original_url:
        #         redirected = False
        # else:
        #     url = url_tools.clean(response.url)
        #     original_url = url
        #
        # url = url_tools.decode(url)
        # original_url = url_tools.decode(original_url)
        url = url_tools.clean(response.url)
        if original_url != url:
            redirected = True

    return url, original_url, redirected, response


def crawl_url(url, value):
    client = pymongo.MongoClient('localhost', 27017)
    database = client.upol_crawler
    try:
        url, original_url, redirected, response = get_url(url)
    except Exception as e:
        raise
    else:
        # Content type is invalid
        if response is None:
            # Set original_url to visited, because original url is invalid.
            if redirected:
                db.set_visited_url(database, url)

            client.close()
            return response, "Response is None", redirected

        if not validator.validate(url):
            client.close()
            return response, "URL is not valid", redirected

        if redirected:
            # Set original_url to visited, because it was redirected
            # db.set_visited_url(original_url)

            if not db.exists_url(database, url):
                if url_tools.is_same_domain(url, original_url):
                    db.insert_url(database, url, True, value - 1)
                else:
                    db.insert_url(database, url, True, config.max_value)
            else:
                if db.is_visited(database, url):
                    return response, "URL is already visited", redirected
                else:
                    db.set_visited_url(database, url)

        # db.set_visited_url(url)

        # # Max depth was reached
        # if value == 0:
        #     client.close()
        #     return response, "URL done - max depth was reached", redirected

        # Begin parse part
        html = response.text
        soup = BeautifulSoup(html, "lxml")

        validated_urls_on_page = parser.validated_page_urls(soup, url)

        for page_url in validated_urls_on_page:
            page_url = url_tools.clean(page_url)

            if url_tools.is_same_domain(url, page_url):
                if value - 1 != 0:
                    db.insert_url(database, page_url, False, value - 1)
            else:
                db.insert_url(database, page_url, False, config.max_value)

            # if not db.exists_url(database, page_url):
            # db.insert_url(database, page_url)

        client.close()
        return response, "URL done", redirected
