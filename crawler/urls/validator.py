from crawler import urls
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
    return config.regex.match(url)
