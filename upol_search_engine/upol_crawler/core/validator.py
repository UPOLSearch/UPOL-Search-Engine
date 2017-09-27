import urllib.parse

from upol_search_engine.upol_crawler.tools import blacklist, robots


def validate_regex(url, regex):
    """Check if url is validate with regex"""
    return regex.match(url)


def validate_anchor(url):
    """Check if url include anchor"""
    cheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    if anchor:
        return False
    else:
        return True


def validate_phpbb(url):
    """Validate if url from phpBB system is valid or blacklisted"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    path = path + qs + anchor

    url_keywords = ['posting.php',
                    'ucp.php',
                    'view=print',
                    'memberlist.php',
                    'mark']

    for url_keyword in url_keywords:
        if url_keyword in path:
            return False

    return True


def validate_wiki(url):
    """Validate if url from wiki system is valid or blacklisted"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    path = path + qs + anchor

    url_keywords = ['&']

    for url_keyword in url_keywords:
        if url_keyword in path:
            return False

    return True


def validate(url, regex, blacklist_list):
    """Complete validator"""
    if not validate_anchor(url):
        return False, 'UrlHasAnchor'

    if not validate_regex(url, regex):
        return False, 'UrlInvalidRegex'

    if blacklist.is_url_blocked(url, blacklist_list):
        return False, 'UrlIsBlacklisted'

    if not robots.is_crawler_allowed(url):
        return False, 'UrlRobotsBlocked'

    return True, None
