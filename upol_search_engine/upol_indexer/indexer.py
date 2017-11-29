import re
import urllib.parse
from io import BytesIO, StringIO

import PyPDF2
from bs4 import BeautifulSoup
from langdetect import detect
from lxml import etree
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage
from upol_search_engine.utils import urls


def extract_content_from_pdf(file_bytes):
    pdf_file = BytesIO(file_bytes)

    pagenums = set()

    info = PyPDF2.PdfFileReader(pdf_file).getDocumentInfo()

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

    title = info.get('/Title')

    if title is not None:
        title = title.replace('\x00', '')
    output.close

    return text, title


def remove_tags_from_string(string):
    text = etree.fromstring(string, etree.HTMLParser())
    parsed = ' '.join(text.xpath("//text()"))

    return parsed


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
    tags_for_remove = ['style',
                       'form',
                       'input',
                       'label',
                       'textarea',
                       'select',
                       'button',
                       'output']

    if body is not None:
        for tag in soup(tags_for_remove):
            tag.extract()
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


def prepare_one_document_for_index(document, limit_domain):
    content = document.get('page').get('content').get('binary')
    url_hash = document.get('representative')
    url = document.get('page').get('url')
    url_decoded = urls.decode(url)
    url_length = len(url)
    is_file = False
    file_type = ""
    depth = document.get('page').get('depth')
    pagerank = document.get('page').get('pagerank')
    language = document.get('page').get('language')

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
    out = fs.get(document.get('page').get('content').get('binary'))
    content = out.read()

    mongodb_client.close()

    url_hash = document.get('representative')
    url = document.get('page').get('url')
    url_decoded = urls.decode(url)
    url_length = len(url)
    is_file = True
    file_type = document.get('page').get('file_type')
    filename = document.get('page').get('filename')
    depth = document.get('page').get('depth')
    pagerank = document.get('page').get('pagerank')

    body_text, title = extract_content_from_pdf(content)

    if (body_text is None) or (len(body_text) < 500):
        return None

    language = detect(body_text)

    if (title is None) or (title is ""):
        title = filename

    description = " "
    keywords = " "
    important_headlines = " "
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
           depth,
           is_file,
           file_type,
           pagerank,
           url_length)

    return row
