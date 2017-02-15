import hashlib
import re
import urllib.parse

import w3lib.url


def hash(url):
    """Returns hash of url"""
    return hashlib.sha1(url.encode('utf-8')).hexdigest()


def hash_document(document):
    """Returns hash of document"""
    return hashlib.sha1(document.encode('utf-8')).hexdigest()


def remove_www(url):
    """Remove www from url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    if scheme is '':
        raise ValueError('Domain has no scheme')

    if netloc[:4] == 'www.':
        netloc = netloc[4:]

    url_without_www = urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor))

    return url_without_www


def clean(url):
    """Remove last backslash from url"""
    url = remove_www(url)
    url = w3lib.url.url_query_cleaner(url, ('sid', 'SID'), remove=True)
    url = w3lib.url.canonicalize_url(url, keep_blank_values=False)
    return url


def is_url_absolute(url):
    """Test if url is absolute"""
    return bool(urllib.parse.urlparse(url).netloc)


def domain(url):
    """Return domain of the url"""
    url = remove_www(url)
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    if scheme is '':
        raise ValueError('Domain has no scheme: {0}'.format(url))

    if ':' in netloc:
        netloc = netloc[:-3]

    return netloc


def is_same_domain(url1, url2):
    """Check if two urls have some domain"""
    return domain(url1) == domain(url2)


def decode(url):
    """Decode and return url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    path = urllib.parse.unquote(path)
    qs = urllib.parse.unquote_plus(qs)
    return urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor))


def generate_regex(url):
    """Generate regex for url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    netloc = netloc.replace('.', '\.')

    return re.compile('^(https?:\/\/)?([a-z0-9]+[.])*'+netloc+'.*$')
