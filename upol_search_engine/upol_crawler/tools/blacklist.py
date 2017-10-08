from upol_search_engine.utils import urls


def generate_blacklist(text):
    return urls.load_urls_from_text(text)


def is_url_blocked(url, blacklist):
    """Check if url domain is blocked"""
    if urls.domain(url) in blacklist:
        return True

    return False
