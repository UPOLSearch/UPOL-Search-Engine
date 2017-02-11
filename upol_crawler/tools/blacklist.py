import urllib.parse

from upol_crawler.utils import urls

# TODO - load values from file
blacklist = ['portal.upol.cz',
             'stag.upol.cz',
             'stagservices.upol.cz',
             'courseware.upol.cz',
             'helpdesk.upol.cz']


def is_url_blocked(url):
    """Check if url domain is blocked"""
    if urls.domain(url) in blacklist:
        return True

    return False
