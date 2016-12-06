import hashlib
import urllib.parse

def hash(url):
    """Returns hash of url"""
    return hashlib.sha1(url.encode('utf-8')).hexdigest()

def clean(url):
    """Remove last backslash from url"""
    return url.rstrip('/')

def domain(url):
    """Return domain of the url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    return netloc
