import hashlib

def hash(url):
    """Returns hash of url"""
    return hashlib.sha1(url.encode('utf-8')).hexdigest()

def clean(url):
    """Remove last backslash from url"""
    return url.rstrip('/')
