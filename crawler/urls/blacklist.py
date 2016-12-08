from crawler.urls import url_tools
import urllib.parse

# TODO - load values from file
blacklist = ["portal.upol.cz",
             "stag.upol.cz",
             "stagservices.upol.cz",
             "courseware.upol.cz",
             "helpdesk.upol.cz"]


def is_url_blocked(url):
    """Check if url domain is blocked"""
    if url_tools.domain(url).replace("www.", "") in blacklist:
        return True

    return False
