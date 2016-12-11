from crawler import tasks
from crawler.db import db_mongodb as db
from time import sleep

db.insert_url("http://www.inf.upol.cz")

while True:
    url = db.random_unvisited_url()

    if url is not None:
        db.set_visited_url(url)
        tasks.crawl_url_task.delay(url)

    # Temporal solution
    if not db.number_of_unvisited_url() > 0:
        number_of_tries = 2000
        while number_of_tries != 0:
            sleep(5)
            if not db.number_of_unvisited_url() > 0:
                number_of_tries = number_of_tries - 1
            else:
                break
