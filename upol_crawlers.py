from crawler import UpolCrawler

logs = "/Users/tomasmikula/crawlers/inf/"
results = "/Users/tomasmikula/crawlers/inf/"
disabled_domains = ["portal.upol.cz", "stag.upol.cz", "stagservices.upol.cz", "courseware.upol.cz", "helpdesk.upol.cz"]

# geology_crawler = UpolCrawler("http://www.geology.upol.cz", results, logs, True)
# geology_crawler.start()

# biology_crawler = UpolCrawler("http://biochemie.upol.cz/", True)
# biology_crawler.start()
#
# ach_crawler = UpolCrawler("http://ach.upol.cz/", results, logs, True)
# ach_crawler.start()
#
# ekologie_crawler = UpolCrawler("http://www.ekologie.upol.cz", results, logs, True)
# ekologie_crawler.start()

# zoologie_crawler = UpolCrawler("http://www.zoologie.upol.cz", results, logs, True)
# zoologie_crawler.start()

# algebra_crawler = UpolCrawler("http://kag.upol.cz", results, logs, True)
# algebra_crawler.start()

# geoinf_crawler = UpolCrawler("http://www.geoinformatics.upol.cz", results, logs, True)
# geoinf_crawler.start()
#
# wiki_test_crawler = UpolCrawler("https://wiki.inf.upol.cz", results, logs, True)
# wiki_test_crawler.start()

# forum_test_crawler = UpolCrawler("https://forum.inf.upol.cz", results, logs, True)
# forum_test_crawler.start()

# inf_crawler = UpolCrawler("http://inf.upol.cz", results, logs, True)
# inf_crawler.start()

upol_crawler = UpolCrawler("http://inf.upol.cz", results, logs, disabled_domains, True)
upol_crawler.start()
