import unittest
import redis
import pymongo
from unittest.mock import patch
import responses
import requests
import urllib.parse
from bs4 import BeautifulSoup

from crawler.urls import url_tools
from crawler.urls import validator
from crawler.urls import blacklist
from crawler.urls import parser
from crawler.db import db_redis as db
from crawler.db import db_mongodb as db


class TestUrlMethods(unittest.TestCase):

    def test_url_clean(self):
        self.assertEqual(url_tools.clean("http://upol.cz/"), "http://upol.cz")
        self.assertEqual(url_tools.clean("http://upol.cz"), "http://upol.cz")

    def test_url_domain(self):
        self.assertEqual(url_tools.domain("http://upol.cz/"), "upol.cz")
        self.assertEqual(url_tools.domain("www.upol.cz/"), "www.upol.cz")
        self.assertEqual(url_tools.domain("www.upol.cz"), "www.upol.cz")

    def test_is_url_absolute(self):
        self.assertTrue(url_tools.is_url_absolute("http://upol.cz/"))
        self.assertFalse(url_tools.is_url_absolute("ahoj/test.jpg"))


@patch('crawler.urls.blacklist.blacklist', ["test.com"])
class TestBlacklistMethods(unittest.TestCase):

    def test_is_url_blocked(self):
        self.assertTrue(blacklist.is_url_blocked("http://test.com/aaa.html"))
        self.assertTrue(blacklist.is_url_blocked("http://test.com"))
        self.assertTrue(blacklist.is_url_blocked("www.test.com"))


class TestParserMethods(unittest.TestCase):

    def test_is_page_wiki(self):
        html = '<meta name="generator" content="MediaWiki 1.26.2">'
        soup = BeautifulSoup(html, "lxml")
        self.assertTrue(parser.is_page_wiki(soup))

        html = '<meta name="generator" content="AAA"><meta name="generator" content="MediaWiki 1.26.2">'
        soup = BeautifulSoup(html, "lxml")
        self.assertTrue(parser.is_page_wiki(soup))

        html = '<html></html>'
        soup = BeautifulSoup(html, "lxml")
        self.assertFalse(parser.is_page_wiki(soup))

    def test_is_page_phpbb(self):
        html = '<body id="phpbb"></body>'
        soup = BeautifulSoup(html, "lxml")
        self.assertTrue(parser.is_page_phpbb(soup))

        html = '<body></body>'
        soup = BeautifulSoup(html, "lxml")
        self.assertFalse(parser.is_page_phpbb(soup))

    def test_base_url(self):
        url = "http://upol.cz/test"
        html = '<html><base href="http://upol.cz/" target="_blank"></html>'
        soup = BeautifulSoup(html, "lxml")

        self.assertEqual(parser.base_url(soup, url), "http://upol.cz/")

        url = "http://upol.cz/test"
        html = '<html></html>'
        soup = BeautifulSoup(html, "lxml")

        self.assertEqual(parser.base_url(soup, url), "http://upol.cz/test")

    def test_validated_page_urls(self):
        url = "http://upol.cz/"
        html = """<a href="ahoj.html">aaa</a>
                  <a href="http://upol.cz/ahoj2.html#test">aaa</a>
                  <a href="http://upol.cz/ahoj.html">aaa</a>
                  <a href="http://twitter.com/ahoj.html">aaa</a>"""
        soup = BeautifulSoup(html, "lxml")

        expected_result = {"http://upol.cz/ahoj.html"}

        self.assertEqual(parser.validated_page_urls(soup, url), expected_result)

    def test_validated_page_urls_base_tag(self):
        url = "http://upol.cz/test"
        html = """<html><base href="http://upol.cz/" target="_blank">
                  <body>
                  <a href="ahoj.html">aaa</a>
                  <a href="http://upol.cz/ahoj2.html">aaa</a>
                  </body>
                  </html>"""
        soup = BeautifulSoup(html, "lxml")

        expected_result = {"http://upol.cz/ahoj.html", "http://upol.cz/ahoj2.html"}

        self.assertEqual(parser.validated_page_urls(soup, url), expected_result)

    def test_validated_page_urls_phpbb(self):
        url = "http://upol.cz/test"
        html = """<html>
                  <body id="phpbb">
                  <div id="page-body">
                  <a href="ahoj.html">aaa</a>
                  </div>
                  <a href="http://upol.cz/ahoj2.html">aaa</a>
                  </body>
                  </html>"""
        soup = BeautifulSoup(html, "lxml")

        expected_result = {"http://upol.cz/ahoj.html"}

        self.assertEqual(parser.validated_page_urls(soup, url), expected_result)


@patch('crawler.urls.validator.content_type_whitelist', ["text/html"])
class TestValidatorMethods(unittest.TestCase):

    @responses.activate
    def test_url_encode(self):
        url = 'http://upol.cz/řeřicha'
        responses.add(responses.GET, 'http://upol.cz/%C5%99e%C5%99icha',
                      body='{"error": "not found"}', status=404,
                      content_type='application/json')

        response = requests.get(url)

        self.assertEqual(url_tools.decode(response.url), url)

    @responses.activate
    def test_content_type(self):
        responses.add(responses.GET, 'http://upol.cz',
                      body='{"error": "not found"}', status=404,
                      content_type='application/json')

        responses.add(responses.GET, 'http://upol2.cz',
                      body='{"error": "not found"}', status=404,
                      content_type='text/html')

        response = requests.get('http://upol.cz')
        self.assertFalse(validator.validate_content_type(response.headers['Content-Type']))

        response = requests.get('http://upol2.cz')
        self.assertTrue(validator.validate_content_type(response.headers['Content-Type']))

    @patch('crawler.urls.validator.file_extension_whitelist', [".php"])
    def test_file_extension(self):
        self.assertTrue(validator.validate_file_extension("http://test.com/index.php"))
        self.assertTrue(validator.validate_file_extension("http://test.com/index"))
        self.assertTrue(validator.validate_file_extension("http://test.com/aaa.jpg/index"))
        self.assertFalse(validator.validate_file_extension("http://test.com/index.jpg"))
        self.assertFalse(validator.validate_file_extension("http://test.com/aaa.jpg/index.jpg"))
        self.assertTrue(validator.validate_file_extension("www.upol.cz"))
        self.assertFalse(validator.validate_file_extension("www.upol.cz/aaaaa.jpg"))

    @patch('crawler.config.regex', url_tools.generate_regex("http://upol.cz"))
    def test_regex(self):
        self.assertTrue(validator.validate_regex("http://upol.cz"))
        self.assertTrue(validator.validate_regex("https://upol.cz"))
        self.assertTrue(validator.validate_regex("www.upol.cz"))
        self.assertTrue(validator.validate_regex("https://www.upol.cz"))
        self.assertTrue(validator.validate_regex("http://upol.cz/"))
        self.assertTrue(validator.validate_regex("https://upol.cz/"))
        self.assertTrue(validator.validate_regex("www.upol.cz/"))
        self.assertTrue(validator.validate_regex("http://inf.upol.cz/"))
        self.assertTrue(validator.validate_regex("https://inf.upol.cz/"))

        self.assertFalse(validator.validate_regex("http://upool.cz"))
        self.assertFalse(validator.validate_regex("https://upool.cz"))
        self.assertFalse(validator.validate_regex("www.upool.cz"))
        self.assertFalse(validator.validate_regex("https://www.upool.cz"))
        self.assertFalse(validator.validate_regex("http://upool.cz/"))
        self.assertFalse(validator.validate_regex("https://upool.cz/"))
        self.assertFalse(validator.validate_regex("www.upool.cz/"))
        self.assertFalse(validator.validate_regex("http://inf.upool.cz/"))
        self.assertFalse(validator.validate_regex("https://inf.upool.cz/"))
        self.assertFalse(validator.validate_regex("htp://upol.cz"))

    def test_validate_anchor(self):
        self.assertTrue(validator.validate_anchor("http://upol.cz"))
        self.assertFalse(validator.validate_anchor("http://upol.cz/asdasd#asdad"))
        self.assertFalse(validator.validate_anchor("www.upol.cz/asdasd#asdad"))

    @patch('crawler.urls.validator.file_extension_whitelist', [".php"])
    @patch('crawler.config.regex', url_tools.generate_regex("http://upol.cz"))
    @patch('crawler.urls.blacklist.blacklist', ["test.com"])
    def test_validator(self):
        self.assertTrue(validator.validate("http://upol.cz/index.php"))
        self.assertTrue(validator.validate("http://upol.cz/index"))
        self.assertTrue(validator.validate("http://upol.cz/aaa.jpg/index"))
        self.assertFalse(validator.validate("http://upol.cz/index.jpg"))
        self.assertFalse(validator.validate("http://upol.cz/aaa.jpg/index.jpg"))

        self.assertTrue(validator.validate("http://upol.cz"))
        self.assertTrue(validator.validate("https://upol.cz"))
        self.assertTrue(validator.validate("www.upol.cz"))
        self.assertTrue(validator.validate("https://www.upol.cz"))
        self.assertTrue(validator.validate("http://upol.cz/"))
        self.assertTrue(validator.validate("https://upol.cz/"))
        self.assertTrue(validator.validate("www.upol.cz/"))
        self.assertTrue(validator.validate("http://inf.upol.cz/"))
        self.assertTrue(validator.validate("https://inf.upol.cz/"))

        self.assertFalse(validator.validate("http://upool.cz"))
        self.assertFalse(validator.validate("https://upool.cz"))
        self.assertFalse(validator.validate("www.upool.cz"))
        self.assertFalse(validator.validate("https://www.upool.cz"))
        self.assertFalse(validator.validate("http://upool.cz/"))
        self.assertFalse(validator.validate("https://upool.cz/"))
        self.assertFalse(validator.validate("www.upool.cz/"))
        self.assertFalse(validator.validate("http://inf.upool.cz/"))
        self.assertFalse(validator.validate("https://inf.upool.cz/"))
        self.assertFalse(validator.validate("htp://upol.cz"))

        self.assertTrue(validator.validate("http://upol.cz"))
        self.assertFalse(validator.validate("http://upol.cz/asdasd#asdad"))


class TesstDbMethodsMongoDb(unittest.TestCase):
    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def setUp(self):
        self.url = "https://forum.inf.upol.cz/viewforum.php?f=18&sid=301bc96d2d47656f0d1a3f82e897a812"
        db.init()

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_insert(self):
        self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
        self.assertEqual(db.insert_url(self.url), False)

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_delete(self):
        self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
        self.assertEqual(db.delete_url(self.url), True)
        self.assertEqual(db.delete_url(self.url), False)

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_exists(self):
        self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
        self.assertEqual(db.exists_url(self.url), True)

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_set_visited(self):
        self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
        self.assertTrue(db.set_visited_url(self.url))
        self.assertFalse(db.set_visited_url(self.url))
        self.assertTrue(db.exists_url(self.url))

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def tearDown(self):
        db.flush_db()

# @patch('crawler.db_redis.db', redis.StrictRedis(host='localhost', port=6379, db=10))
# @patch('crawler.db_redis.db_visited', redis.StrictRedis(host='localhost', port=6379, db=11))
# class TestDbMethods(unittest.TestCase):
#     def setUp(self):
#         self.url = "http://upol.cz"
#
#     def test_url_insert(self):
#         self.assertFalse(db_redis.exists_url(self.url))
#         self.assertTrue(db_redis.insert_url(self.url))
#         self.assertTrue(db_redis.exists_url(self.url))
#
#     def test_url_delete(self):
#         db_redis.insert_url("http://test.com")
#         self.assertEqual(db_redis.delete_url("http://test.com"), True)
#         self.assertEqual(db_redis.delete_url("Not exists"), False)
#
#     def test_url_exists(self):
#         db_redis.insert_url(self.url)
#         self.assertTrue(db_redis.exists_url(self.url))
#         self.assertFalse(db_redis.exists_url("Not exists"))
#
#     def test_url_set_visited(self):
#         self.assertFalse(db_redis.exists_url(self.url))
#         self.assertFalse(db_redis.set_visited_url(self.url))
#         db_redis.insert_url(self.url)
#         self.assertTrue(db_redis.set_visited_url(self.url))
#         self.assertTrue(db_redis.exists_url(self.url))
#
#     def test_url_random(self):
#         db_redis.insert_url("http://test.com")
#         db_redis.insert_url("http://test2.com")
#         db_redis.insert_url("http://test3.com")
#         self.assertEqual(db_redis.random_unvisited_url(), "")
#
#     def tearDown(self):
#         db_redis.flush_db()

if __name__ == '__main__':
    unittest.main()
