import logging
import re
import urllib.parse
from io import BytesIO, StringIO

import PyPDF2
from bs4 import BeautifulSoup
from langdetect import detect
from lxml import etree
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage
from pdfminer.pdftypes import PDFException
from pdfminer.psparser import PSSyntaxError
from PyPDF2 import utils
from upol_search_engine.utils import document, urls


def utf8len(s):
    return len(s.encode('utf-8'))


def extract_content_from_pdf(file_bytes):
    logging.propagate = False
    logging.getLogger().setLevel(logging.ERROR)

    pdf_file = BytesIO(file_bytes)

    pagenums = set()

    # try:
    #     info = PyPDF2.PdfFileReader(pdf_file).getDocumentInfo()
    # except utils.PdfReadError as e:
    #     info = {'/Title': ""}

    output = StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)
    pages = PDFPage.get_pages(pdf_file, pagenums)

    for page in pages:
        interpreter.process_page(page)

    converter.close()

    text = output.getvalue()

    if text is not None:
        text = text.replace('ˇ', '').replace('’', '').replace('´', '').replace('˚', '').replace('ı', 'i').replace('\x00', '')

    output.close

    return text


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
            document.remove_tags_from_string(result))
    else:
        return result


def extract_words_from_url(url, limit_domain):
    words = []
    parsed = urllib.parse.urlparse(url)

    netloc = parsed.netloc.lower()
    netloc = netloc.replace('www.', '')
    netloc_old, netloc = netloc, netloc.replace(limit_domain, '')
    if netloc == '':
        netloc = netloc_old.replace(limit_domain.split('.')[-1], '')

    netloc = netloc.strip('.')
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


def extract_body_text(soup):
    body = soup.find('body')
    tags_for_remove = ['style',
                       'form',
                       'input',
                       'label',
                       'textarea',
                       'select',
                       'button',
                       'output',
                       'script']

    classes_for_remove = ['hidden',
                          'hide']

    if body is not None:
        for tag in soup(tags_for_remove):
            tag.extract()

        for tag in soup.find_all(True, {'class': classes_for_remove}):
            tag.decompose()

        for hidden in soup.find_all(style=re.compile(r'display:\s*none')):
            hidden.decompose()

        body_text = document.remove_tags_from_string(body.prettify())
        body_text = replace_new_line_and_spaces_by_dot(body_text)

    else:
        body_text = ""

    return body_text


def prepare_one_document_for_index(document, limit_domain):
    content = document.get('content').get('binary')
    content_hash = document.get('content').get('hashes').get('text')
    url_hash = document.get('_id')
    url = document.get('url')
    url_decoded = urls.decode(url)
    url_length = len(url)
    is_file = False
    file_type = None
    depth = document.get('depth')
    pagerank = document.get('pagerank')
    language = document.get('language')

    soup = BeautifulSoup(content, 'html5lib')

    for script in soup('script'):
        script.extract()

    title = extract_title(soup)

    if title is None:
        return None

    body_text = extract_body_text(soup)

    if len(body_text) < 500:
        return None

    description = extract_description(soup)
    keywords = extract_keywords(soup)
    important_headlines = extract_important_headlines(soup)
    url_words = ' '.join(extract_words_from_url(url_decoded, limit_domain))

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
           content_hash,
           depth,
           is_file,
           file_type,
           pagerank,
           url_length)

    return row


def prepare_one_file_for_index(document, limit_domain):
    import gridfs
    from upol_search_engine.db import mongodb

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(limit_domain,
                                            mongodb_client)
    fs = gridfs.GridFS(mongodb_database)
    out = fs.get(document.get('content').get('binary'))
    content = out.read()

    mongodb_client.close()

    content_hash = document.get('content').get('hashes').get('text')
    url_hash = document.get('_id')
    url = document.get('url')
    url_decoded = urls.decode(url)
    url_length = len(url)
    is_file = True
    file_type = document.get('file_type')
    filename = document.get('filename')
    depth = document.get('depth')
    pagerank = document.get('pagerank')

    body_text = extract_content_from_pdf(content)

    # Reduce size of body_text for database
    while utf8len(body_text) > 900000:
        body_text = body_text[:-10000]

    if (body_text == "") or (body_text is None) or (len(body_text) < 500):
        return None

    # Add later, performance problem
    # language = detect(body_text)
    language = 'cs'

    title = filename

    description = ""
    keywords = ""
    important_headlines = ""
    url_words = ' '.join(extract_words_from_url(url_decoded, limit_domain))

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
           content_hash,
           depth,
           is_file,
           file_type,
           pagerank,
           url_length)

    return row
