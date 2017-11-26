import hashlib
import os.path
import re
import urllib.parse

import w3lib.url


def hash(url):
    """Returns hash of url"""
    return hashlib.sha1(url.encode('utf-8')).hexdigest()


def hash_document(document):
    """Returns hash of document"""
    return hashlib.sha1(document).hexdigest()


def remove_www(url):
    """Remove www from url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    if scheme is '':
        raise ValueError('Domain has no scheme')

    if netloc[:4] == 'www.':
        netloc = netloc[4:]

    url_without_www = urllib.parse.urlunsplit((scheme, netloc,
                                               path, qs, anchor))

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


def get_filename(url):
    """Return filename from url"""
    path = urllib.parse.urlparse(url).path

    return os.path.basename(path)


def domain(url):
    """Return domain of the url"""
    url = remove_www(url)
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    if scheme is '':
        raise ValueError('Domain has no scheme: {0}'.format(url))

    if ':' in netloc:
        netloc = netloc.split(':', 1)[0]

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


def generate_regex(domain):
    """Generate regex for domain"""
    return re.compile('^(https?:\/\/)?([a-z0-9]+[.])*' + domain + '.*$')


def load_urls_from_file(filepath):
    """Load urls from file, one per line,
    ignore lines with #, ignores duplicity"""
    urls = set()

    if not os.path.isfile(filepath):
        return urls

    with open(filepath) as url_file:
        for line in url_file:
            # Ignore all white characters
            url = line.strip()
            # Take url only if is not commented
            if not line.startswith("#") and (url != '') and (url is not None):
                urls.add(url)

    return urls


def load_urls_from_text(text):
    """Load urls from text, one per line,
    ignore lines with #, ignores duplicity"""
    urls = set()

    lines = text.split('\n')

    for line in lines:
        # Ignore all white characters
        url = line.strip()
        # Take url only if is not commented
        if not line.startswith("#") and (url != '') and (url is not None):
            urls.add(url)

    return urls


def domain_replace_dots(domain):
    """Simple function which replace . in domain by -"""
    return domain.replace('.', '-')
