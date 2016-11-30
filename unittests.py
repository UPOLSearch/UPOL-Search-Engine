import unittest
from crawler import UpolCrawler
import urllib.parse
import urllib.request
import urllib.error
import sqlite3
from bs4 import BeautifulSoup
import time
import datetime
import re
import hashlib
from retry import retry
import os
import sys
import time

logs = "/Users/tomasmikula/crawlers/tests/"
results = "/Users/tomasmikula/crawlers/tests/"

class TestCrawler(unittest.TestCase):
    def setUp(self):
        self.logs = "/Users/tomasmikula/crawlers/tests/"
        self.results = "/Users/tomasmikula/crawlers/tests/"
        self.disabled_domains = ["portal.upol.cz", "stag.upol.cz", "stagservices.upol.cz", "courseware.upol.cz", "helpdesk.upol.cz"]
        self.url = "http://www.upol.cz/"
        self.crawler = UpolCrawler(self.url, self.results, self.logs, self.disabled_domains, False)

    def test_url_remove_www(self):
        self.assertEqual(self.crawler.url_remove_www("https://reddit.com/r/Art"), "https://reddit.com/r/Art")
        self.assertEqual(self.crawler.url_remove_www("http://www.inkluze.upol.cz/portal/wp-content/uploads/2013/10/Manual_k_prime_podpore-verze-07www.pdf"), "http://inkluze.upol.cz/portal/wp-content/uploads/2013/10/Manual_k_prime_podpore-verze-07www.pdf")
        self.assertEqual(self.crawler.url_remove_www("https://www.reddit.com/r/Art"), "https://reddit.com/r/Art")

    def test_url_clean(self):
        self.assertEqual(self.crawler.url_clean("https://reddit.com/r/Art/"), "https://reddit.com/r/Art")
        self.assertEqual(self.crawler.url_clean("http://www.inkluze.upol.cz/portal/wp-content/uploads/2013/10/Manual_k_prime_podpore-verze-07www.pdf/"), "http://inkluze.upol.cz/portal/wp-content/uploads/2013/10/Manual_k_prime_podpore-verze-07www.pdf")
        self.assertEqual(self.crawler.url_clean("https://www.reddit.com/r/Art/"), "https://reddit.com/r/Art")

    def test_url_validator(self):
        self.assertTrue(self.crawler.url_validator("http://oltk.upol.cz/knihovna"))
        self.assertTrue(self.crawler.url_validator("http://oltk.upol.cz/knihovna/"))
        self.assertTrue(self.crawler.url_validator("http://www.oltk.upol.cz/knihovna/"))
        self.assertTrue(self.crawler.url_validator("https://www.oltk.upol.cz/knihovna/"))

    def test_page_base_url(self):
        html1 = '<head><base href="http://www.w3schools.com/images/" target="_blank"></head>'
        html2 = '<head><base target="_blank"></head>'
        soup1 = BeautifulSoup(html1, "lxml")
        soup2 = BeautifulSoup(html2, "lxml")
        self.assertEqual(self.crawler.page_base_url(soup1, "http://www.w3schools.com/"), "http://www.w3schools.com/images/")
        self.assertEqual(self.crawler.page_base_url(soup2, "http://www.w3schools.com/"), "http://www.w3schools.com/")


if __name__ == '__main__':
    unittest.main()
