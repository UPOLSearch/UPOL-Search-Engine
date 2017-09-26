# import urllib.parse

# from upol_search_engine.upol_crawler import settings
from upol_search_engine.upol_crawler.utils import urls


# blacklist = urls.load_urls_from_file(settings.BLACKLIST_FILE)


# def load_blacklist(blacklist_path):
#     """Load blacklisted domains from file"""
#     blacklist = urls.load_urls_from_file(blacklist_path)
#
def generate_blacklist(text):
    return urls.load_urls_from_text(text)
    

def is_url_blocked(url, blacklist):
    """Check if url domain is blocked"""
    if urls.domain(url) in blacklist:
        return True

    return False
