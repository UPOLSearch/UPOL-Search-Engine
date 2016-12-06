import unittest
from crawler import url

class TestUrlMethods(unittest.TestCase):

    def test_url_clean(self):
        self.assertEqual(url.clean("http://upol.cz/"), "http://upol.cz")
        self.assertEqual(url.clean("http://upol.cz"), "http://upol.cz")

if __name__ == '__main__':
    unittest.main()
