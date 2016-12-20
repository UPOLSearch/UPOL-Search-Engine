import pymongo
from crawler.urls import url_tools
from bson import json_util
import json

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
    client = pymongo.MongoClient('localhost', 27017)
    database = client.upol_crawler

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

def log_url_validator(url, validator):
    client = pymongo.MongoClient('localhost', 27017)
    database = client.upol_crawler

    if validator == "blacklist":
        log_object = {"_id": url_tools.hash(url),
                      "url": url,
                      "blacklisted": True}

    elif validator == "robots_block":
        log_object = {"_id": url_tools.hash(url),
                      "url": url,
                      "robots_block": True}

    try:
        database.urls_logs_not_valid.insert_one(log_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    client.close()
