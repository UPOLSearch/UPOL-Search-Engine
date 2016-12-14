from crawler import tasks
from crawler.db import db_mongodb as db
import pymongo
from time import sleep
import datetime

from celery.app.control import Control
from crawler.celery import app


def is_worker_running():
    inspect = app.control.inspect()

    active = inspect.active()
    scheduled = inspect.scheduled()
    reserved = inspect.reserved()

    if active is not None:
        active_number = len(list(active.values())[0])
    else:
        active_number = 0
    if scheduled is not None:
        scheduled_number = len(list(scheduled.values())[0])
    else:
        scheduled_number = 0
    if reserved is not None:
        reserved_number = len(list(reserved.values())[0])
    else:
        reserved_number = 0

    if active_number + scheduled_number + reserved_number > 0:
        return True
    else:
        return False

# start_time = datetime.datetime.now()

client = pymongo.MongoClient('localhost', 27017)
database = client.upol_crawler

# Temporal solution
db.init(database)
db.insert_url(database, "http://www.upol.cz")
db.insert_url(database, "http://www.cmtf.upol.cz")
db.insert_url(database, "http://www.lf.upol.cz")
db.insert_url(database, "http://www.ff.upol.cz")
db.insert_url(database, "http://www.prf.upol.cz")
db.insert_url(database, "http://www.pdf.upol.cz")
db.insert_url(database, "http://ftk.upol.cz")
db.insert_url(database, "http://www.pf.upol.cz")
db.insert_url(database, "http://www.fzv.upol.cz")

start_time = datetime.datetime.now()
sleeping = False

while True:
    end_time = datetime.datetime.now()
    elapsed = end_time - start_time

    if elapsed.seconds >= 10 and sleeping is True:
        sleeping = False
        
    if sleeping is False:
        url = db.get_unvisited_url(database)

        if url is not None:
            sleeping = False
            print("FEEDING QUEUE")
            db.set_visited_url(database, url)
            # db.inser_url_visited(database, url)
            tasks.crawl_url_task.delay(url)
        else:
            print("WORKER IS RUNNING - SLEEPING")
            sleeping = True
            start_time = datetime.datetime.now()


        # sleep(2)
        # if is_worker_running():
        #     print("WORKER IS RUNNING - SLEEPING")
        #     sleep(5)
        # else:
        #     print("END")
        #     break

# end_time = datetime.datetime.now()
# elapsed = end_time - start_time
# print(str(elapsed))
