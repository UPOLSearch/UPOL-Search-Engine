"""
Universal database functions.
specific functions related to some components are in component file
"""

import random
import urllib.parse
from datetime import datetime
from random import shuffle

import gridfs
import pymongo
from bson.objectid import ObjectId
from upol_crawler.settings import *
from upol_crawler.utils import urls


def init(db):
    """Database init, create indexes"""
    db['Urls'].create_index('visited')
    db['Urls'].create_index('indexed')
    db['Urls'].create_index('noindex')
    db['Urls'].create_index('file')
    db['Urls'].create_index('invalid')
    db['Urls'].create_index('queued')
    db['Urls'].create_index('timeout')
    db['Urls'].create_index('canonical_group')
    db['Limiter'].create_index('ip', unique=True)
    db['PageRank'].create_index([('from_hash', pymongo.DESCENDING),
                                 ('to_hash', pymongo.ASCENDING)], unique=True)


def _prepare_url_object(url, visited, queued, depth):
    """Prepare url object before inserting into database"""
    url_object = {'_id': urls.hash(url),
                  'url': url,
                  'domain': urls.domain(url),
                  'depth': depth,
                  'visited': visited,
                  'queued': queued,
                  'alias': False,
                  'invalid': False,
                  'file': False,
                  'progress': {'discovered': str(datetime.now())}}

    return url_object


# def _prepare_file_url_object(url, response, depth):
#     """Prepare file url object before inserting into database"""
#     # Prepare content_type, remove charset etc
#     # for example 'text/html; charset=utf-8'
#     content_type = response.headers.get('Content-Type')
#
#     if content_type is not None:
#         content_type = content_type.split(';')[0]
#
#     file_url_object = {'_id': urls.hash(url),
#                        'url': url,
#                        'domain': urls.domain(url),
#                        'depth': depth,
#                        'content_type': content_type,
#                        'content_length': response.headers.get('Content-Length'),
#                        'progress': {'discovered': str(datetime.now())}}
#
#     return file_url_object


def insert_url(db, url, visited, queued, depth):
    """Insert url into db"""
    url_object = _prepare_url_object(url, visited, queued, depth)

    try:
        result = db['Urls'].insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result


# def inser_file_url(db, url, response, depth):
#     """Insert file url into db"""
#     file_url_object = _prepare_file_url_object(url, response, depth)
#
#     try:
#         result = db['Files'].insert_one(file_url_object).inserted_id
#     except pymongo.errors.DuplicateKeyError as e:
#         return False
#
#     return result


def batch_insert_url(db, urls_with_depths, visited, queued):
    """Inser batch of urls into db"""

    url_documents = []

    for url in urls_with_depths:
        url_object = _prepare_url_object(url.get('url'),
                                         visited,
                                         queued,
                                         url.get('depth'))
        url_documents.append(url_object)

    try:
        result = db['Urls'].insert_many(url_documents, ordered=False)
    except pymongo.errors.BulkWriteError:
        # TODO - There is no point of returning result variable from this function. insert_many can fail on one url because of duplicity and thats totally fine. So probably better to ignore return statement
        result = None

    return result


# def iterate_inlinks(db, url):
#     """Iterate number of inlinks of one url"""
#
#     return db['Urls'].find_one_and_update({'_id': urls.hash(url)},
#                                           {'$inc': {'inlinks': 1}})


# def insert_pagerank_outlink(db, from_url, to_url):
#     """Insert outlink into database"""
#
#     result = db['PageRank'].insert_one({urls.hash(from_url), urls.hash(to_url)}})
#
#     return result

def batch_insert_pagerank_outlinks(db, from_url, to_urls):
    """Inser batch of outlinks into database"""

    url_documents = []

    for to_url in to_urls:
        to_url = to_url.get('url')
        url_object = {'from_hash': urls.hash(from_url),
                      'to_hash': urls.hash(to_url)}

        url_documents.append(url_object)

    try:
        result = db['PageRank'].insert_many(url_documents, ordered=False)
    except pymongo.errors.BulkWriteError:
        # TODO - There is no point of returning result variable from this function. insert_many can fail on one url because of duplicity and thats totally fine. So probably better to ignore return statement
        result = None

    return result


# def update_pagerank_url_hash(db, original_hash, new_hash):
#     """Update url hash in graph's edge if canonical group is changed"""
#
#     try:
#         result1 = db['PageRank'].update_many({'from_hash': original_hash}, {'$set': {'from_hash': new_hash}})
#         result2 = db['PageRank'].update_many({'to_hash': original_hash}, {'$set': {'to_hash': new_hash}})
#     except pymongo.errors.DuplicateKeyError as e:
#         pass
#
#
#     return result1.raw_result, result2.raw_result


def delete_pagerank_edge_to(db, to_hash):
    """Delete edge from pagerank"""

    result = db['PageRank'].delete_many({'to_hash': to_hash})

    return result.deleted_count > 0


# def insert_url_info(db, url, info_type, arg={}):
#     """Insert aditional info about url into database"""
#     collection = db[info_type]
#
#     log_object = {'_id': urls.hash(url),
#                   'url': url}
#
#     for key, depth in arg.items():
#         log_object[key] = str(depth)
#
#     try:
#         collection.insert_one(log_object).inserted_id
#     except pymongo.errors.DuplicateKeyError as e:
#         return False


def delete_url(db, url):
    """Try to delete url from db, returns True if case of success"""
    result = db['Urls'].delete_one({'_id': urls.hash(url)})

    return result.deleted_count > 0


def get_or_create_canonical_group(db, text_hash):
    """Try to get canonical group with given hash.
       Create new canonical g roup in case of fail.
       Canonical group groups url with same text hash, not HTML tags."""

    # TODO - Possible chance of optimalization here
    canonical_group = list(db['CanonicalGroups'].find({'text_hash': text_hash}).limit(1))

    # Create new one
    if len(canonical_group) == 0:
        return db['CanonicalGroups'].insert({'text_hash': text_hash})
    else:
        return canonical_group[0].get('_id')


# def get_or_create_duplicity_group(db, content_hash):
#     """Try to get duplicity group with given hash.
#        Create new duplicity group in case of fail."""
#
#     # TODO - Possible chance of optimalization here
#     duplicity_group = list(db['DuplicityGroups'].find({'content_hash': content_hash}).limit(1))
#
#     # Create new one
#     if len(duplicity_group) == 0:
#         return db['DuplicityGroups'].insert({'content_hash': content_hash})
#     else:
#         return duplicity_group[0].get('_id')


def select_representative_for_canonical_group(db, canonical_group):
    """Return id of URL which is suitable as representative of canonical group"""

    urls_representatives = db['Urls'].find({'canonical_group': ObjectId(canonical_group), 'alias': False, 'invalid': False})

    representatives = []

    for url in urls_representatives:
        representatives.append(url.get('url'))

    # Return hash of the shortest url
    return urls.hash(min(representatives, key=len))


def update_canonical_group_representative(db, canonical_group, representative):
    """Update representative url of canonical group"""

    return db['CanonicalGroups'].find_one_and_update({'_id': ObjectId(canonical_group)},
                         {'$set': {'representative': representative}})


def set_alias_visited_url(db, url):
    url_hash = urls.hash(url)

    url_addition = {}

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['alias'] = True
    url_addition['progress.last_visited'] = str(datetime.now())

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    return result is not None

def set_visited_invalid_url(db, url, response, reason):
    url_hash = urls.hash(url)

    url_addition = {}

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['invalid'] = True
    url_addition['invalid_reason'] = reason
    url_addition['progress.last_visited'] = str(datetime.now())

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    return result is not None


def _determine_type_of_redirect(response):
    is_permanent_redirect = False

    for history in response.history:
        if history.is_permanent_redirect:
            is_permanent_redirect = True
            break

    is_redirect = False

    for history in response.history:
        if history.is_redirect:
            is_redirect = True
            break

    return is_redirect, is_permanent_redirect


def _set_canonical_group_to_alias(db, original_url, canonical_group):
    """If there was redirect, set the canonical group to the orginal alias url"""

    modification = {'canonical_group': canonical_group}
    return db['Urls'].find_one_and_update({'_id': urls.hash(original_url)}, {'$set': modification})


def _update_representatives_of_canonical_groups(db, canonical_group):
    """If insertion was successful update representative of canonical group"""
    
    representative = select_representative_for_canonical_group(db, canonical_group)
    return update_canonical_group_representative(db, canonical_group, representative)


def set_visited_file_url(db, url, response, original_url=None):
    """Save file into database and set is as visited"""

    url_hash = urls.hash(url)

    is_redirect, is_permanent_redirect = _determine_type_of_redirect(response)

    url_addition = {}

    # Pairing url with canonical group id
    content_hash = urls.hash_document(response.content)
    url_addition['canonical_group'] = get_or_create_canonical_group(db, content_hash)

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['indexed'] = False
    url_addition['noindex'] = False
    url_addition['file'] = True

    url_addition['progress.last_visited'] = str(datetime.now())

    # GridFS connection
    fs = gridfs.GridFS(db)
    file_id = fs.put(response.content)

    url_addition['content.binary'] = file_id

    url_addition['content.hashes.content'] = content_hash

    url_addition['response.elapsed'] = str(response.elapsed)
    url_addition['response.is_redirect'] = is_redirect
    url_addition['response.is_permanent_redirect'] = is_permanent_redirect
    url_addition['response.status_code'] = response.status_code
    url_addition['response.reason'] = response.reason

    for key, value in response.headers.items():
        url_addition['response.' + str(key)] = str(value)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    # If there was redirect, set the canonical group to the orginal alias url
    if original_url is not None:
        _set_canonical_group_to_alias(db, original_url, url_addition['canonical_group'])

    # If insertion was successful update representative of canonical group
    if result is not None:
        _update_representatives_of_canonical_groups(db, url_addition['canonical_group'])

    return result is not None

def set_visited_url(db, url, response, soup, noindex, original_url=None):
    """Try to set url to visited and update other important informations"""
    url_hash = urls.hash(url)

    is_redirect, is_permanent_redirect = _determine_type_of_redirect(response)

    url_addition = {}

    # Pairing url with canonical group id
    text_hash = urls.hash_document(soup.text.encode())
    url_addition['canonical_group'] = get_or_create_canonical_group(db, text_hash)

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['indexed'] = False
    url_addition['noindex'] = noindex

    url_addition['progress.last_visited'] = str(datetime.now())

    url_addition['content.binary'] = response.content
    url_addition['content.hashes.text'] = text_hash
    url_addition['content.encoding'] = response.encoding
    # Later detect language

    url_addition['response.elapsed'] = str(response.elapsed)
    url_addition['response.is_redirect'] = is_redirect
    url_addition['response.is_permanent_redirect'] = is_permanent_redirect
    url_addition['response.status_code'] = response.status_code
    url_addition['response.reason'] = response.reason

    for key, value in response.headers.items():
        url_addition['response.' + str(key)] = str(value)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    # If there was redirect, set the canonical group to the orginal alias url
    if original_url is not None:
        _set_canonical_group_to_alias(db, original_url, url_addition['canonical_group'])

    # If insertion was successful update representative of canonical group
    if result is not None:
        _update_representatives_of_canonical_groups(db, url_addition['canonical_group'])


    return result is not None


# def set_redirected_url(db, url):
#     """Try to set url as redirected"""
#     url_hash = urls.hash(url)
#
#     result = db['Urls'].find_one_and_update({'_id': url_hash},
#                                             {'$set': {'redirected': True}})
#
#     return result is not None


# def set_queued_url(db, url):
#     """Try to set url to queued"""
#     url_hash = urls.hash(url)
#
#     result = db['Urls'].find_one_and_update({'_id': url_hash},
#                                             {'$set': {'queued': True}})
#
#     return result is not None


def set_queued_batch(db, list_url_hash):
    """Try to set batch of urls to queued"""

    result = db['Urls'].update_many({'_id': {'$in': list_url_hash}},
                                    {'$set': {'queued': True}})

    return result is not None


def set_url_for_recrawl(db, url):
    """Set url for recrawl later"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': {'queued': False,
                                                      'visited': False}})

    return result is not None


def set_timeout_url(db, url):
    """Try to set url as timouted"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': {
                                                'queued': False,
                                                'timeout.timeout': True,
                                                'timeout.last_timeout': str(datetime.now())
                                                }})

    return result is not None


def get_batch_url_for_crawl(db, size):
    """Return batch of url from db for crawl"""
    db_batch = list(db['Urls'].aggregate([{'$match':
                                           {'$and': [
                                               {'visited': False},
                                               {'queued': False},
                                               {'timeout': {
                                                   '$exists': False}}]}},
                                          {'$sample': {'size': size}}]))

    if len(db_batch) != 0:
        batch = []

        for field in db_batch:
            url = {'_id': field.get('_id'),
                   'url': field.get('url'),
                   'depth': field.get('depth')}

            batch.append(url)
            shuffle(batch)

        return batch
    else:
        return None


def exists_url(db, url):
    """Return if url is exists in db"""
    url_hash = urls.hash(url)

    result = db['Urls'].find_one({'_id': url_hash})

    return result is not None


# def is_visited(db, url):
#     """Check if url is visited"""
#     result = db['Urls'].find_one({'visited': True})
#
#     if result is not None:
#         return True


# def is_queued(db, url):
#     """Check if url is queued"""
#     result = db['Urls'].find_one({'queued': True})
#
#     if result is not None:
#         return True


# def is_visited_or_queued(db, url):
#     """Check if url is visited or queued"""
#     result = db['Urls'].find_one({'$or': [
#                                 {'visited': True},
#                                 {'queued': True}
#                               ]})
#
#     if result is not None:
#         return True
