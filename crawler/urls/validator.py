from crawler.urls import blacklist
from crawler.urls import url_tools
from crawler.urls import robots
from crawler.db import db_mongodb as db
import urllib.parse
from crawler import config
import pymongo
import crawler

# TODO - load values from file
content_type_whitelist = ["text/html"]
file_extension_whitelist = [".php",
                            ".html",
                            ".xhtml",
                            ".htm"]


def validate_content_type(content_type_header):
    """Validate if content-type is in content-type whitelist"""
    for content_type in content_type_whitelist:
        if content_type in content_type_header:
            return True

    return False


def validate_file_extension(url):
    """Check if url include blacklisted file extension"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    # In case of www.upol.cz
    # TODO - Maybe implement in higher layer
    if not scheme:
        return validate_file_extension(url_tools.add_scheme(url))

    path_split = path.split('/')

    if "." in path_split[-1]:
        valid = False
        for file_extension in file_extension_whitelist:
            if file_extension in path_split[-1]:
                valid = True
                break
    else:
        valid = True

    return valid


def validate_regex(url):
    """Check if url is validate with regex"""
    return config.regex.match(url)


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
    path = path+qs+anchor

    url_keywords = ["posting.php", "ucp.php", "view=print", "memberlist.php", "mark"]

    for url_keyword in url_keywords:
        if url_keyword in path:
            return False

    return True


def validate_wiki(url):
    """Validate if url from wiki system is valid or blacklisted"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    path = path+qs+anchor

    url_keywords = ["&"]

    for url_keyword in url_keywords:
        if url_keyword in path:
            return False

    return True

    
def validate(url):
    """Complete validator"""
    if not validate_anchor(url):
        return False, "UrlHasAnchor"

    if not validate_regex(url):
        return False, "UrlInvalidRegex"

    if blacklist.is_url_blocked(url):
        return False, "UrlIsBlacklisted"

    if not robots.is_crawler_allowed(url):
        return False, "UrlRobotsBlocked"

    # Need to be last
    if not validate_file_extension(url):
        return False, "UrlIsFile"

    return True, None
