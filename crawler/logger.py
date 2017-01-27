import pymongo
import json
from bson import json_util
from crawler.urls import url_tools
from crawler.db import db_mongodb as db


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


# Deprecated
def log_url_validator(url, validator, arg=None):
    client = pymongo.MongoClient('localhost', 27017, maxPoolSize=None)
    database = client.upol_crawler

    if validator == "blacklist":
        log_object = {
                      "url": url,
                      "blacklisted": True}

    elif validator == "robots_block":
        log_object = {
                      "url": url,
                      "robots_block": True}

    elif validator == "exception":
        log_object = {
                      "url": url,
                      "exception": True,
                      "value": str(arg)}

    elif validator == "not_valid_redirect":
        log_object = {
                      "url": url,
                      "not_valid_redirect": True}

    elif validator == "anchor":
        log_object = {
                      "url": url,
                      "anchor": True}

    elif validator == "regex":
        log_object = {
                      "url": url,
                      "regex": True}

    elif validator == "rel":
        log_object = {
                      "url": url,
                      "rel": True}

    elif validator == "visiting":
        log_object = {
                      "url": url,
                      "visiting": True}

    elif validator == "visited":
        log_object = {
                      "url": url,
                      "visited": True}

    elif validator == "parsing":
        log_object = {
                      "url": url,
                      "parsing": True,
                      "valid_urls": str(arg)}

    elif validator == "redirected":
        log_object = {
                      "url": url,
                      "redirected": True,}


    try:
        database.urls_logs_not_valid.insert_one(log_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    client.close()
