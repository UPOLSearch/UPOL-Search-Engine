import hashlib
import urllib.parse
import re

def hash(url):
    """Returns hash of url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    return hashlib.sha1(url.encode('utf-8')).hexdigest()

def clean(url):
    """Remove last backslash from url"""
    return url.rstrip('/')

def domain(url):
    """Return domain of the url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    return netloc

def generate_regex(url):
    """Generate regex for url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    netloc = netloc.replace(".", "\.")

    return re.compile("^(https?:\/\/)?([a-z0-9]+[.])*"+netloc+".*$")
