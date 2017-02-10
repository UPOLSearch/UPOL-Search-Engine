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
