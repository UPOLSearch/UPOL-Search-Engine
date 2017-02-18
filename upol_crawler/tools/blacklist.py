import urllib.parse

from upol_crawler.utils import urls
from upol_crawler.settings import *


blacklist = urls.load_urls_from_file(BLACKLIST_FILE)


# def load_blacklist(blacklist_path):
#     """Load blacklisted domains from file"""
#     blacklist = urls.load_urls_from_file(blacklist_path)
#


def is_url_blocked(url):
    """Check if url domain is blocked"""
    if urls.domain(url) in blacklist:
        return True

    return False
