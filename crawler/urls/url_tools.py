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

def add_scheme(url):
    """Add missing scheme to url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    scheme = "http"
    netloc = path
    path = ""
    return urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor))

def domain(url):
    """Return domain of the url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    #TODO - Maybe implement in higher layer
    if not scheme:
        url = add_scheme(url)
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    return netloc

def generate_regex(url):
    """Generate regex for url"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    netloc = netloc.replace(".", "\.")

    return re.compile("^(https?:\/\/)?([a-z0-9]+[.])*"+netloc+".*$")
