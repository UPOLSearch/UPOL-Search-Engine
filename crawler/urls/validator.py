from crawler.urls import blacklist
from crawler.urls import url_tools
import urllib.parse
from crawler import config

#TODO - load values from file
content_type_whitelist = ["text/html"]
file_extension_whitelist = [".php",
                            ".html",
                            ".xhtml",]

def validate_content_type(content_type_header):
    """Validate if content-type is in content-type whitelist"""
    for content_type in content_type_whitelist:
        if content_type in content_type_header:
            return True

    return False

def validate_file_extension(url):
    """Check if url include blacklisted file extension"""
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)

    #In case of www.upol.cz
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

def validate(url):
    """Complete validator"""
    if not validate_anchor(url):
        return False

    if not validate_regex(url):
        return False

    if not validate_file_extension(url):
        return False

    if blacklist.is_url_blocked(url):
        return False

    return True
