import unittest
import redis
from unittest.mock import patch

from crawler import url
from crawler import db

class TestUrlMethods(unittest.TestCase):

    def test_url_clean(self):
        self.assertEqual(url.clean("http://upol.cz/"), "http://upol.cz")
        self.assertEqual(url.clean("http://upol.cz"), "http://upol.cz")

@patch('crawler.db.db', redis.Redis(host='localhost', port=6379, db=10))
@patch('crawler.db.db_visited', redis.Redis(host='localhost', port=6379, db=11))
class TestDbMethods(unittest.TestCase):
    def setUp(self):
        self.url = "http://upol.cz"

    def test_url_inser(self):
        self.assertFalse(db.exists_url(self.url))
        self.assertTrue(db.insert_url(self.url))
        self.assertTrue(db.exists_url(self.url))

    def test_url_delete(self):
        db.insert_url("http://test.com")
        self.assertEqual(db.delete_url("http://test.com"), True)
        self.assertEqual(db.delete_url("Not exists"), False)

    def test_url_exists(self):
        db.insert_url(self.url)
        self.assertTrue(db.exists_url(self.url))
        self.assertFalse(db.exists_url("Not exists"))

    def test_url_set_visited(self):
        self.assertFalse(db.exists_url(self.url))
        self.assertFalse(db.set_visited_url(self.url))
        db.insert_url(self.url)
        self.assertTrue(db.set_visited_url(self.url))
        self.assertTrue(db.exists_url(self.url))

    def tearDown(self):
        db.flush_db()

if __name__ == '__main__':
    unittest.main()
