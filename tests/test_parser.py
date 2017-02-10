import pytest
from bs4 import BeautifulSoup

from upol_crawler.urls import parser


@pytest.mark.parametrize('html, result', [
    ('<meta name="generator" content="MediaWiki 1.26.2">', True),
    ('<meta name="generator" content="MediaWiki 1.2">', True),
    ('<meta name="generator" content="MediaWiki">', True),
    ('<meta name="generator" content="AAA"><meta name="generator" content="MediaWiki 1.26.2">', True),
    ])
def test_is_page_wiki(html, result):
    soup = BeautifulSoup(html, "lxml")
    assert parser.is_page_wiki(soup) == result


@pytest.mark.parametrize('html, result', [
    ('<a href="aaaa" rel="nofollow">', False),
    ('<a href="aaaa" rel="bookmark">', False),
    ('<a href="aaaa" rel="alternate">', False),
    ('<a href="aaaa" rel="license">', False),
    ('<a href="aaaa" rel="search">', False),
    ('<a href="aaaa">', True),
    ])
def test_check_rel_attribute(html, result):
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all('a', href=True)
    assert parser.check_rel_attribute(links[0]) == result
