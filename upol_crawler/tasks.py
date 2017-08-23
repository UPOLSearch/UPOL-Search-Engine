import cProfile
import hashlib
import os
import time
from datetime import datetime

import pymongo
from upol_crawler import db
from upol_crawler.celery_app import app
from upol_crawler.settings import *
from upol_crawler.tools import logger

# log_collector = logger.universal_logger('collector')
log_crawler = logger.universal_logger('crawl_url')

@app.task(rate_limit=CONFIG.get('Settings', 'crawl_frequency'), queue='crawler', ignore_result=True, task_compression='zlib')
def crawl_url_task(url, depth):
    try:
        from upol_crawler.core import crawler

        if CONFIG.getboolean('Debug', 'cprofile_crawl_task'):
            os.makedirs(CPROFILE_DIR, exist_ok=True)

            actual_time = str(datetime.now()).encode('utf-8')

            cprofile_filename = hashlib.sha1(actual_time).hexdigest()
            cprofile_path = os.path.join(CPROFILE_DIR, cprofile_filename)

            cProfile.runctx('crawler.crawl_url(url, depth)',
                            globals=globals(),
                            locals=locals(),
                            filename=cprofile_path)
        else:
            crawler.crawl_url(url, depth)
    except Exception as e:
        log_crawler.exception('Exception: {0}'.format(url))
        raise


# @app.task(queue='collector', ignore_result=True, task_compression='zlib')
# def collect_url_info_task(url, info_type, args={}):
#     try:
#         client = pymongo.MongoClient(
#           CONFIG.get('Database', 'db_server'),
#           int(CONFIG.get('Database', 'db_port')),
#           maxPoolSize=None)
#         database = client[DATABASE_NAME]
#
#         db.insert_url_info(database, url, info_type, args)
#
#         client.close()
#     except Exception as e:
#         log_collector.exception('Exception: {0}'.format(url))
#         raise
