from crawler import tasks
from crawler.db import db_mongodb as db
from time import sleep
import datetime

from celery.app.control import Control
from crawler.celery import app

# Temporal solution
db.insert_url("http://www.upol.cz")
db.insert_url("http://www.cmtf.upol.cz")
db.insert_url("http://www.lf.upol.cz")
db.insert_url("http://www.ff.upol.cz")
db.insert_url("http://www.prf.upol.cz")
db.insert_url("http://www.pdf.upol.cz")
db.insert_url("http://ftk.upol.cz")
db.insert_url("http://www.pf.upol.cz")
db.insert_url("http://www.fzv.upol.cz")


def is_worker_running():
    inspect = app.control.inspect()

    active = inspect.active()
    scheduled = inspect.scheduled()
    reserved = inspect.reserved()

    active_number = len(list(active.values())[0])
    scheduled_number = len(list(scheduled.values())[0])
    reserved_number = len(list(reserved.values())[0])

    if active_number + scheduled_number + reserved_number > 0:
        return True
    else:
        return False

start_time = datetime.datetime.now()

while True:
    url = db.random_unvisited_url()

    if url is not None:
        print("FEEDING QUEUE")
        db.set_visited_url(url)
        tasks.crawl_url_task.delay(url)
    else:
        if is_worker_running():
            print("WORKER IS RUNNING - SLEEPING")
            sleep(2)
        else:
            print("END")
            break

end_time = datetime.datetime.now()
elapsed = end_time - start_time
print(str(elapsed))
