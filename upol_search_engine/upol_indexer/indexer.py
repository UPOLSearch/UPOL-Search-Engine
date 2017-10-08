from bs4 import BeautifulSoup
from upol_search_engine.utils import urls


def extract_title(soup):
    if soup.title is not None:
        title = soup.title.string
        soup.find('title').replaceWith('')
    else:
        title = 'No Title'

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


def prepare_one_document_for_index(document):
    content = document.get('page').get('content').get('binary')
    url_hash = document.get('representative')
    url = document.get('page').get('url')
    url_length = len(url)
    is_file = document.get('page').get('file')
    depth = document.get('page').get('depth')
    pagerank = document.get('page').get('pagerank')
    language = document.get('page').get('language')

    soup = BeautifulSoup(content, 'lxml')

    for script in soup('script'):
        script.extract()

    title = extract_title(soup)
    description = extract_description(soup)
    keywords = extract_keywords(soup)

    row = (url_hash,
           url,
           urls.decode(url),
           title,
           language,
           keywords,
           description,
           str(soup),
           depth,
           is_file,
           pagerank,
           url_length)

    return row
