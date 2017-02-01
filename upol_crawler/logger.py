import json

import pymongo
from bson import json_util
from upol_crawler.db import db_mongodb as db
from upol_crawler.urls import url_tools


def get_log_format(response):
    log = {}

    formated_headers = {}

    for key, value in response.headers.items():
        formated_headers[str(key)] = str(value)

    log['encoding'] = response.encoding
    log['elapsed'] = str(response.elapsed)
    log['headers'] = formated_headers
    log['is_permanent_redirect'] = response.is_permanent_redirect
    log['is_redirect'] = response.is_redirect
    log['status_code'] = response.status_code
    log['reason'] = response.reason

    return log


def log_url(url, response):
    client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
    database = client.upol_crawler
    # database = db.database

    log_object = {"_id": url_tools.hash(url),
                  "url": url,
                  "encoding": response.get('encoding'),
                  "elapsed": response.get('elapsed'),
                  "headers": response.get('headers'),
                  "is_permanent_redirect": response.get('is_permanent_redirect'),
                  "is_redirect": response.get('is_redirect'),
                  "status_code": response.get('status_code'),
                  "reason": response.get('reason')}

    try:
        database.urls_logs.insert_one(log_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False


    client.close()

def log_url_reason(url, reason, arg={}):
    client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
    database = client.upol_crawler
    collection = database[reason]

    log_object = {"_id": url_tools.hash(url),
                  "url": url}

    for key, value in arg.items():
        log_object[key] = str(value)

    try:
        collection.insert_one(log_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    client.close()
