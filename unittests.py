import unittest
import urllib.parse
from unittest.mock import patch

import pymongo
import redis
import requests
import responses
from bs4 import BeautifulSoup
from upol_crawler import crawler
from upol_crawler.db import db_mongodb as db
from upol_crawler.db import db_redis as db
from upol_crawler.urls import blacklist, parser, robots, url_tools, validator


class TestRobotsMethods(unittest.TestCase):
    def test_robots(self):
        url = "http://inf.upol.cz/"
        self.assertEqual(robots.is_crawler_allowed(url), True)

    def test_robots(self):
        url = "http://inf.upol.cz/languages/"
        self.assertEqual(robots.is_crawler_allowed(url), False)


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

    def test_is_same_domain(self):
        self.assertTrue(url_tools.is_same_domain("http://upol.cz/lol", "http://upol.cz/lol2"))
        self.assertTrue(url_tools.is_same_domain("https://upol.cz/lol", "http://upol.cz/lol2"))
        self.assertTrue(url_tools.is_same_domain("http://www.upol.cz/lol", "http://upol.cz/lol2"))
        self.assertFalse(url_tools.is_same_domain("http://upol.cz/lol", "http://upol2.cz/lol2"))

    @responses.activate
    def test_url_encode(self):
        url = 'http://upol.cz/řeřicha'
        responses.add(responses.GET, 'http://upol.cz/%C5%99e%C5%99icha',
                      body='{"error": "not found"}', status=404,
                      content_type='application/json')

        response = requests.get(url)

        self.assertEqual(url_tools.decode(response.url), url)

    @responses.activate
    def test_url_encode(self):
        url = 'http://upol.cz/'
        responses.add(responses.GET, 'http://upol.cz/',
                      body='{"error": "not found"}', status=404,
                      content_type='application/json')

        response = requests.get(url)

        self.assertEqual(url_tools.decode(response.url), url)


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

    @patch('crawler.urls.validator.file_extension_whitelist', [".html"])
    @patch('crawler.config.regex', url_tools.generate_regex("http://upol.cz"))
    @patch('crawler.urls.blacklist.blacklist', ["test.com"])
    def test_validated_page_urls(self):
        url = "http://upol.cz/"
        html = """<a href="ahoj.html">aaa</a>
                  <a href="http://upol.cz/ahoj2.html#test">aaa</a>
                  <a href="http://upol.cz/ahoj.html">aaa</a>
                  <a href="http://twitter.com/ahoj.html">aaa</a>"""
        soup = BeautifulSoup(html, "lxml")

        expected_result = {"http://upol.cz/ahoj.html"}

        self.assertEqual(parser.validated_page_urls(soup, url), expected_result)

    @patch('crawler.urls.validator.file_extension_whitelist', [".html"])
    @patch('crawler.config.regex', url_tools.generate_regex("http://upol.cz"))
    @patch('crawler.urls.blacklist.blacklist', ["test.com"])
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

    @patch('crawler.urls.validator.file_extension_whitelist', [".html"])
    @patch('crawler.config.regex', url_tools.generate_regex("http://upol.cz"))
    @patch('crawler.urls.blacklist.blacklist', ["test.com"])
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

    def test_check_rel(self):
        html = '<a href="aaaa" rel="nofolow">'
        soup = BeautifulSoup(html, "lxml")
        links = soup.find_all('a', href=True)
        self.assertTrue(parser.check_rel_attribute(links[0]), False)

        html = '<a href="aaaa">'
        soup = BeautifulSoup(html, "lxml")
        links = soup.find_all('a', href=True)
        self.assertTrue(parser.check_rel_attribute(links[0]), True)

    def test_check_meta_robots(self):
        html = '<meta name=“robots“ content=“noindex, nofollow“>'
        soup = BeautifulSoup(html, "lxml")
        self.assertTrue(parser.check_meta_robots(soup), False)

        html = '<meta name=“robots“ content=“noindex“>'
        soup = BeautifulSoup(html, "lxml")
        self.assertTrue(parser.check_meta_robots(soup), True)


@patch('crawler.urls.validator.content_type_whitelist', ["text/html"])
class TestValidatorMethods(unittest.TestCase):

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
        self.assertTrue(validator.validate_file_extension("https://upol.cz"))
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

    @patch('crawler.config.regex', url_tools.generate_regex("http://inf.upol.cz"))
    def test_validator_inf(self):
        self.assertTrue(validator.validate("http://www.inf.upol.cz/vyzkum/archiv-prednasek?akce=DAMOL Seminar&rok=2014"))


# class TesstDbMethodsMongoDb(unittest.TestCase):
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def setUp(self):
#         self.url = "https://forum.inf.upol.cz/viewforum.php?f=18&sid=301bc96d2d47656f0d1a3f82e897a812"
#         db.init()
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def test_url_insert(self):
#         self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
#         self.assertEqual(db.insert_url(self.url), False)
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def test_url_delete(self):
#         self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
#         self.assertEqual(db.delete_url(self.url), True)
#         self.assertEqual(db.delete_url(self.url), False)
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def test_url_exists(self):
#         self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
#         self.assertEqual(db.exists_url(self.url), True)
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def test_url_set_visited(self):
#         self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
#         self.assertTrue(db.set_visited_url(self.url))
#         self.assertFalse(db.set_visited_url(self.url))
#         self.assertTrue(db.exists_url(self.url))
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def test_number_of_unvisited(self):
#         self.assertEqual(db.insert_url(self.url), url_tools.hash(self.url))
#         self.assertEqual(db.insert_url(self.url + "/aaa"), url_tools.hash(self.url + "/aaa"))
#         self.assertEqual(db.number_of_unvisited_url(),  2)
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def test_random_unvisited_url(self):
#         self.assertEqual(db.random_unvisited_url(), None)
#
#     @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
#     def tearDown(self):
#         db.flush_db()

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
