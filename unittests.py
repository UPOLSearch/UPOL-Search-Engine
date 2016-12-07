import unittest
import redis
from unittest.mock import patch

from crawler import url
from crawler.db import db_redis
from crawler import blacklist

class TestUrlMethods(unittest.TestCase):

    def test_url_clean(self):
        self.assertEqual(url.clean("http://upol.cz/"), "http://upol.cz")
        self.assertEqual(url.clean("http://upol.cz"), "http://upol.cz")

    def test_url_domain(self):
        self.assertEqual(url.domain("http://upol.cz/"), "upol.cz")

@patch('crawler.blacklist.blacklist', ["test.com"])
class TestBlacklistMethods(unittest.TestCase):

    def test_is_url_blocked(self):
        self.assertTrue(blacklist.is_url_blocked("http://test.com/aaa.html"))
        self.assertTrue(blacklist.is_url_blocked("http://test.com"))

# @patch('crawler.db_redis.db', redis.StrictRedis(host='localhost', port=6379, db=10))
# @patch('crawler.db_redis.db_visited', redis.StrictRedis(host='localhost', port=6379, db=11))
# class TestDbMethods(unittest.TestCase):
#     def setUp(self):
#         self.url = "http://upol.cz"
#
#     def test_url_inser(self):
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
