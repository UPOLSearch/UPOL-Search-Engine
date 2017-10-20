import re
import urllib.parse
from os.path import splitext

from bs4 import BeautifulSoup
from lxml import etree

from upol_search_engine.utils import urls


def remove_tags_from_string(string):
    text = etree.fromstring(string, etree.HTMLParser())
    parsed = ' '.join(text.xpath("//text()"))

    return parsed


def delete_all_tags_from_soup(soup, tag):
    tags = soup.find_all(tag)

    for t in tags:
        t.replaceWith('')

    return soup


def remove_multiple_newlines_and_spaces(string):
    string = string.replace('\n', ' ')
    string = re.sub(' +', ' ', string)

    return string.strip()


def replace_new_line_and_spaces_by_dot(string):
    string = re.sub('\n +', '·', string)
    string = re.sub(' +', ' ', string)
    string = re.sub('·+', ' · ', string)
    string = string.replace('\n', '')
    string = string.strip('· ')
    string = string.strip(' ')

    return string


def extract_title(soup):
    title = soup.find('title')

    if title is not None:
        soup.find('title').replaceWith('')
        title = title.text
    else:
        title = None

    return title


def extract_keywords(soup):
    meta_keywords = soup.find('meta', {'name': 'keywords'})
    keywords = ""

    if meta_keywords is not None:
        keywords = meta_keywords.get('content')
        meta_keywords.replaceWith('')

    return keywords


def extract_description(soup):
    meta_description = soup.find('meta', {'name': 'description'})
    description = ""

    if meta_description is not None:
        description = meta_description.get('content')
        meta_description.replaceWith('')

    return description


def extract_important_headlines(soup):
    important_headline = ['h1', 'h2']

    headlines = soup.find_all(important_headline)

    result = ""

    for headline in headlines:
        result += headline.prettify()

    if result != "":
        return remove_multiple_newlines_and_spaces(
            remove_tags_from_string(result))
    else:
        return result


def extract_body_text(soup):
    body = soup.find('body')

    if body is not None:
        body = delete_all_tags_from_soup(body, 'style')
        body = delete_all_tags_from_soup(body, 'form')
        body_text = remove_tags_from_string(body.prettify())
        body_text = replace_new_line_and_spaces_by_dot(body_text)
    else:
        body_text = ""

    return body_text


def extract_words_from_url(url, limit_domain):
    words = []
    parsed = urllib.parse.urlparse(url)

    netloc = parsed.netloc.lower()
    netloc = netloc.replace('www.', '')
    netloc = netloc.replace(limit_domain, '')
    netloc = netloc.strip('.')
    words.extend(re.split(r'-|_|\.|\(|\)|:', netloc))

    path = parsed.path.lower()
    path = re.sub(r'(index|page|help)\.[a-z]+', '', path)
    path = re.sub(r'\.(php|html?|aspx)', '', path)
    words.extend(re.split(r'/+|-|_|\.|\(|\)|:', path))

    query = urllib.parse.parse_qsl(parsed.query)

    for key, value in query:
        words.extend(extract_words_from_url(value, limit_domain))

    blacklist = ['mailto', 'home', 'en', 'cs', 'de']

    words = [x for x in words if x not in blacklist]

    return list(filter(None, words))


def prepare_one_document_for_index(document):
    content = document.get('page').get('content').get('binary')
    url_hash = document.get('representative')
    url = document.get('page').get('url')
    url_decoded = urls.decode(url)
    url_length = len(url)
    is_file = document.get('page').get('file')
    depth = document.get('page').get('depth')
    pagerank = document.get('page').get('pagerank')
    language = document.get('page').get('language')

    soup = BeautifulSoup(content, 'html5lib')

    for script in soup('script'):
        script.extract()

    title = extract_title(soup)

    if title is None:
        return None

    description = extract_description(soup)
    keywords = extract_keywords(soup)
    important_headlines = extract_important_headlines(soup)
    body_text = extract_body_text(soup)
    url_words = ' '.join(extract_words_from_url(url_decoded))

    row = (url_hash,
           url,
           url_decoded,
           url_words,
           title,
           language,
           keywords,
           description,
           important_headlines,
           body_text,
           depth,
           is_file,
           pagerank,
           url_length)

    return row
