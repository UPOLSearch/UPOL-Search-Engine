import pytest
from upol_crawler.urls import url_tools


@pytest.mark.parametrize('url, domain', [
    ("http://upol.cz/", "upol.cz"),
    ("http://upol.cz", "upol.cz"),
    ("https://upol.cz", "upol.cz"),
    ("https://upol.cz/", "upol.cz"),
    ("https://www.upol.cz/", "upol.cz"),
    pytest.mark.raises(("www.upol.cz/", "upol.cz"), exception=ValueError),
    ])
def test_url_domain(url, domain):
    assert url_tools.domain(url) == domain


@pytest.mark.parametrize('url, expected', [
    ("http://upol.cz/", True),
    ("http://upol.cz/absbdss", True),
    ("ahoj/test.jpg", False),
    ("www.aha.com/ahoj/test.jpg", False),
    ])
def test_is_url_absolute(url, expected):
    assert url_tools.is_url_absolute(url) == expected


@pytest.mark.parametrize('url1, url2, expected', [
    ("http://upol.cz/lol", "http://upol.cz/lol2", True),
    ("https://upol.cz/lol", "http://upol.cz/lol2", True),
    ("https://upol.cz/lol", "http://upol.cz/lol2", True),
    ])
def test_is_same_domain(url1, url2, expected):
    assert url_tools.is_same_domain(url1, url2) == expected


@pytest.mark.parametrize('url, expected', [
    ("http://upol.cz/", "http://upol.cz/"),
    ("http://www.upol.cz", "http://upol.cz"),
    ("http://upolwww.cz", "http://upolwww.cz"),
    ("http://upol.cz/testwww.html", "http://upol.cz/testwww.html"),
    ("http://www.upol.cz/testwww.html", "http://upol.cz/testwww.html"),
    pytest.mark.raises(("www.upol.cz/", "upol.cz"), exception=ValueError),
    ])
def test_remove_www(url, expected):
    assert url_tools.remove_www(url) == expected
