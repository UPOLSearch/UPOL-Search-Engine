import unittest
import redis
import pymongo
from unittest.mock import patch

from crawler import urls
from crawler.db import db_redis as db
from crawler.db import db_mongodb as db
from crawler import blacklist

class TestUrlMethods(unittest.TestCase):

    def test_url_clean(self):
        self.assertEqual(urls.clean("http://upol.cz/"), "http://upol.cz")
        self.assertEqual(urls.clean("http://upol.cz"), "http://upol.cz")

    def test_url_domain(self):
        self.assertEqual(urls.domain("http://upol.cz/"), "upol.cz")

@patch('crawler.blacklist.blacklist', ["test.com"])
class TestBlacklistMethods(unittest.TestCase):

    def test_is_url_blocked(self):
        self.assertTrue(blacklist.is_url_blocked("http://test.com/aaa.html"))
        self.assertTrue(blacklist.is_url_blocked("http://test.com"))

class TesstDbMethodsMongoDb(unittest.TestCase):
    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def setUp(self):
        self.url = "https://forum.inf.upol.cz/viewforum.php?f=18&sid=301bc96d2d47656f0d1a3f82e897a812"
        db.init()

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_insert(self):
        self.assertEqual(db.insert_url(self.url), urls.hash(self.url))
        self.assertEqual(db.insert_url(self.url), False)

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_delete(self):
        self.assertEqual(db.insert_url(self.url), urls.hash(self.url))
        self.assertEqual(db.delete_url(self.url), True)
        self.assertEqual(db.delete_url(self.url), False)

    @patch('crawler.db.db_mongodb.db', pymongo.MongoClient('localhost', 27017).upol_crawler_test)
    def test_url_exists(self):
        self.assertEqual(db.insert_url(self.url), urls.hash(self.url))
        self.assertEqual(db.exists_url(self.url), True)

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
