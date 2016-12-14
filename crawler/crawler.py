import requests
from bs4 import BeautifulSoup
from crawler import config
from crawler.urls import validator
from crawler.urls import parser
from crawler.urls import url_tools
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
        if len(response.history) > 0:
            redirected = True
            original_url = url
            url = url_tools.clean(response.history[0].url)

            if url == original_url:
                redirected = False
        else:
            url = url_tools.clean(response.url)
            original_url = url

        url = url_tools.decode(url)
        original_url = url_tools.decode(original_url)

    return url, original_url, redirected, response


def crawl_url(url):
    try:
        url, original_url, redirected, response = get_url(url)
    except Exception as e:
        raise
    else:
        if response is None:
            # Set original_url to visited, because original url is invalid.
            if redirected:
                db.set_visited_url(url)
            return response, "Response is None", redirected

        if not validator.validate(url):
            return response, "URL is not valid", redirected

        if redirected:
            # Set original_url to visited, because it was redirected
            # db.set_visited_url(original_url)

            if not db.exists_url(url):
                db.insert_url(url)
            else:
                if db.is_visited(url):
                    return response, "URL is already visited", redirected
                else:
                    db.set_visited_url(url)

        # db.set_visited_url(url)

        # Begin parse part
        html = response.text
        soup = BeautifulSoup(html, "lxml")

        validated_urls_on_page = parser.validated_page_urls(soup, url)

        for page_url in validated_urls_on_page:
            page_url = url_tools.clean(page_url)

            if not db.exists_url(page_url):
                db.insert_url(page_url)

        return response, "URL done", redirected
