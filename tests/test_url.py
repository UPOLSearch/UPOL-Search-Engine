import pytest
from upol_crawler.urls import url_tools


@pytest.mark.parametrize("url, domain", [
    ("http://upol.cz/", "upol.cz"),
    ("http://upol.cz", "upol.cz"),
    ("https://upol.cz", "upol.cz"),
    ("https://upol.cz/", "upol.cz"),
    pytest.mark.xfail(("www.upol.cz/", "upol.cz")),
    pytest.mark.xfail(("www.upol.cz", "upol.cz")),
    ])
def test_url_domain(url, domain):
    assert url_tools.domain(url) == domain

@pytest.mark.parametrize("url, expected", [
    ("http://upol.cz/", True),
    pytest.mark.xfail(("ahoj/test.jpg", False)),
    ])
def test_is_url_absolute(url, expected):
    assert url_tools.is_url_absolute(url) == expected
