import hashlib
import re

from lxml import etree


def hash_document(document):
    """Returns hash of document"""
    return hashlib.sha1(document).hexdigest()


def remove_tags_from_string(string):
    text = etree.fromstring(string, etree.HTMLParser())
    parsed = ' '.join(text.xpath("//text()"))

    return parsed


def extract_document_text_for_hash(soup):
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

    # stat-block is removed because of phpBB duplicity
    classes_for_remove = ['hidden',
                          'hide',
                          'stat-block']

    if body is None:
        body = soup.find('html')

        if body is None:
            document_text = soup.text.encode()
            return document_text

    for tag in soup(tags_for_remove):
        tag.extract()

    for tag in soup.find_all(True, {'class': classes_for_remove}):
        tag.decompose()

    for hidden in soup.find_all(style=re.compile(r'display:\s*none')):
        hidden.decompose()

    body_text = body.text

    if body_text is None or body_text == "":
        document_text = soup.text.encode()
    else:
        try:
            document_text = remove_tags_from_string(body.text).encode()
        except Exception as e:
            document_text = body.text.encode()

    return document_text
