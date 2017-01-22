import urllib.parse
import re
import crawler
from crawler.urls import validator
from crawler.urls import url_tools


def is_page_wiki(soup):
    """Detect if page is wiki, from soup"""

    meta_generators = soup.find_all('meta', {'name': 'generator'})

    for meta_generator in meta_generators:
        content = meta_generator['content']
        if "MediaWiki" in content:
            return True

    return False


def is_page_phpbb(soup):
    """Detect if page is phpBB, from soup"""
    return (soup.find('body', id='phpbb') is not None)


def base_url(soup, url):
    """Get base url from page (check if base html tag exists)"""
    base_url = url
    base_tag = soup.find_all('base', href=True)

    if len(base_tag) > 0:
        base_url = base_tag[0]['href']

    return base_url


def wiki_page(soup):
    """Parse wiki page and return valid links"""

    content_div = soup.find('div', id='content')

    for div in content_div.find_all('div', {'class': 'printfooter'}):
        div.decompose()

    links_tmp = content_div.find_all('a', href=True)
    links = set()

    for link in links_tmp:
        if validator.validate_wiki(link['href']):
            links.add(link)

    return links


def phpBB_page(soup):
    """Parse phpBB page and return valid links"""

    content_div = soup.find('div', id='page-body')

    for p in content_div.find_all('p', {'class': 'jumpbox-return'}):
        p.decompose()

    links_tmp = content_div.find_all('a', href=True)
    links = set()

    for link in links_tmp:
        if validator.validate_phpbb(link['href']):
            links.add(link)

    return links


def link_extractor(soup, url):
    """Extract all links from page"""

    if is_page_wiki(soup):
        return wiki_page(soup)

    elif is_page_phpbb(soup):
        return phpBB_page(soup)

    else:
        return set(soup.find_all('a', href=True))


def check_rel_attribute(link):
    """Check rel attribute of link"""

    rel = link.get('rel')

    if "nofollow" in rel:
        return False
    elif "bookmark" in rel:
        return False
    elif "alternate" in rel:
        return False
    elif "license" in rel:
        return False
    elif "search" in rel:
        return False
    else:
        return True


def check_meta_robots(soup):
    """Check meta tag robots"""
    meta = soup.find("meta", {"name": "robots"})

    if meta is not None:
        content = meta.get('content')
        if "nofollow" in content:
            return False
        else:
            return True
    else:
        return True


def get_canonical_url(soup):
    """Return canonical url if exists"""
    # <link rel="canonical" href="https://forum.inf.upol.cz/viewforum.php?f=18">
    link = soup.find("link", {"rel": "canonical"})

    if link is not None:
        url = link.get('href')
        url.replace("http://", "")
        url.replace("https://", "")
        url.replace("www", "")
        return url
    else:
        return None


def validated_page_urls(soup, url):
    """Parse page and return set of valid urls"""

    valid_urls = set()

    # Check if page has meta robots tag
    if not check_meta_robots(soup):
        return valid_urls

    links_on_page = link_extractor(soup, url)
    page_base_url = base_url(soup, url)
    canonical_url = get_canonical_url(soup)

    for link in links_on_page:
        crawler.tasks.log_url_reason_task.delay(str(link), "Links")
        # if has some rel attributes - ignore
        if not check_rel_attribute(link):
            crawler.tasks.log_url_reason_task.delay(link['href'], "UrlRelBlocked", {"type": link['rel']})
            continue

        link_url = link['href']

        if not url_tools.is_url_absolute(link_url):
            link_url = urllib.parse.urljoin(page_base_url, link_url)

        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(link_url)

        if not scheme:
            link_url = url_tools.add_scheme(url)

        link_url = url_tools.clean(link_url)

        if canonical_url is not None:
            # If canonical url is part of url, skip this url
            if canonical_url in link_url:
                continue

        valid, reason = validator.validate(link_url)

        if valid:
            link_url = url_tools.remove_sid(link_url)
            valid_urls.add(link_url)
        else:
            if reason == "UrlIsFile" or reason == "UrlRobotsBlocked":
                crawler.tasks.log_url_reason_task.delay(url, reason)

    return valid_urls
