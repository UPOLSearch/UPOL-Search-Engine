import re

from bs4 import BeautifulSoup
from flask import Blueprint, render_template, request
from upol_search_engine import settings
from upol_search_engine.upol_crawler.core import (crawler, link_extractor,
                                                  validator)
from upol_search_engine.upol_indexer import microformat
from upol_search_engine.utils import urls

mod = Blueprint('info', __name__, url_prefix='/info')


@mod.route('/')
def home():
    return render_template('info/home.html')


@mod.route('/datamining')
def datamining():
    return render_template('info/datamining.html')


@mod.route('/debugger')
def debugger():

    page = request.args.get('page')

    if page is None:
        page = 'http://inf.upol.cz'

    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    if url_regex.match(page):
        is_url_valid = True
    else:
        return render_template('info/debugger.html', **locals())

    # Add try except later
    blacklist = urls.load_urls_from_file(settings.blacklist_path)
    regex = urls.generate_regex(
        settings.CONFIG.get('Crawler', 'limit_domain'))

    is_valid, reason = validator.validate(page, regex, blacklist)

    if is_valid:
        url, original_url, is_redirect, response = crawler.get_page(
            page,
            settings.CONFIG.getfloat('Crawler', 'connect_max_timeout'),
            settings.CONFIG.getint('Crawler', 'read_max_timeout'))

        if is_redirect:
            is_valid_redirect, reason_redirect = validator.validate(
                url,
                regex,
                blacklist)

        content_type = response.headers.get('Content-Type')

        is_file = crawler.test_content_type_file(content_type)
        is_valid_file = crawler.test_file_valid_type(content_type)

        if not is_file:
            soup = BeautifulSoup(response.content, 'html5lib')

            is_wiki = link_extractor.is_page_wiki(soup)
            is_phpbb = link_extractor.is_page_phpbb(soup)
            is_nofollow = not link_extractor.check_meta_robots(soup)
            is_noindex = link_extractor.has_noindex(soup)

            accepted_links, not_accepted_links = link_extractor.validated_page_urls(
                soup, url, regex, blacklist)

            metadata = microformat.find_microformat_on_page(soup)

            if metadata is not None:
                parsed_metadata = microformat.parse_json(metadata)
                is_metadata_valid = microformat.validate_json_schema(
                    parsed_metadata)

        return render_template('info/debugger.html', **locals())
