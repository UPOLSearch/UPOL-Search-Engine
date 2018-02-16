from datetime import datetime
from random import shuffle

import gridfs
import pymongo
import pytz
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from langdetect import detect
from upol_search_engine import settings
from upol_search_engine.utils import document, urls


def create_client():
    client = pymongo.MongoClient(
        settings.CONFIG.get('General', 'mongo_db_server'),
        settings.CONFIG.getint('General', 'mongo_db_port'),
        username=settings.CONFIG.get('General', 'mongo_db_user'),
        password=settings.CONFIG.get('General', 'mongo_db_password'),
        authSource='admin',
        authMechanism='SCRAM-SHA-1',
        maxPoolSize=None)

    return client


def get_database(limit_domain, client):
    database_name = urls.domain_replace_dots(limit_domain)
    database = client[database_name]

    return database


def get_stats_database(client):
    return client["stats"]


def drop_database(db_name):
    client = create_client()
    client.drop_database(db_name)


def init(db):
    """Database init, create indexes"""
    db['Urls'].create_index('visited')
    db['Urls'].create_index('indexed')
    db['Urls'].create_index('noindex')
    db['Urls'].create_index('file')
    db['Urls'].create_index('file_type')
    db['Urls'].create_index('invalid')
    db['Urls'].create_index('queued')
    db['Urls'].create_index('timeout')
    db['Urls'].create_index('alias')
    db['Urls'].create_index('canonical_group')
    db['Limiter'].create_index('ip')
    db['PageRank'].create_index('to_hash')
    db['PageRank'].create_index([('from_hash', pymongo.DESCENDING),
                                 ('to_hash', pymongo.DESCENDING)], unique=True)


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
                  'progress': {'discovered': str(datetime.utcnow())}}

    return url_object


def insert_url(db, url, visited, queued, depth):
    """Insert url into db"""
    url_object = _prepare_url_object(url, visited, queued, depth)

    try:
        result = db['Urls'].insert_one(url_object).inserted_id
    except pymongo.errors.DuplicateKeyError as e:
        return False

    return result


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
        result = None

    return result


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
        result = None

    return result


def delete_pagerank_edge_to(db, to_hash):
    """Delete edge from pagerank"""

    result = db['PageRank'].delete_many({'to_hash': to_hash})

    return result.deleted_count > 0


def delete_url(db, url):
    """Try to delete url from db, returns True if case of success"""
    result = db['Urls'].delete_one({'_id': urls.hash(url)})

    return result.deleted_count > 0


def get_or_create_canonical_group(db, text_hash):
    """Try to get canonical group with given hash.
       Create new canonical g roup in case of fail.
       Canonical group groups url with same text hash, not HTML tags."""

    # TODO - Possible chance of optimalization here
    canonical_group = list(db['CanonicalGroups'].find(
        {'text_hash': text_hash}).limit(1))

    # Create new one
    if len(canonical_group) == 0:
        return db['CanonicalGroups'].insert({'text_hash': text_hash})
    else:
        return canonical_group[0].get('_id')


def get_url(db, url):
    document = db['Urls'].find_one({'_id': urls.hash(url)})

    return document


def get_document_by_id(db, document_id):
    document = db['Urls'].find_one({'_id': document_id})

    return document


def get_batch_by_id(db, id_list):
    result = db['Urls'].find({'_id': {'$in': id_list}})

    return result


def select_representative_for_canonical_group(db, canonical_group):
    """Return id of URL which is suitable
    as representative of canonical group"""

    urls_representatives = db['Urls'].find(
        {'canonical_group': ObjectId(canonical_group),
         'alias': False,
         'invalid': False})

    representatives = []

    for url in urls_representatives:
        representatives.append(url.get('url'))

    # Return hash of the shortest url
    return urls.hash(min(representatives, key=len))


def update_canonical_group_representative(db, canonical_group, representative):
    """Update representative url of canonical group"""

    return db['CanonicalGroups'].find_one_and_update(
        {'_id': ObjectId(canonical_group)},
        {'$set': {'representative': representative}})


def set_alias_visited_url(db, url):
    url_hash = urls.hash(url)

    url_addition = {}

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['alias'] = True
    url_addition['progress.last_visited'] = str(datetime.utcnow())

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    return result is not None


def set_visited_invalid_url(db, url, response, reason, is_file=False):
    url_hash = urls.hash(url)

    url_addition = {}

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['invalid'] = True
    url_addition['file'] = is_file
    url_addition['invalid_reason'] = reason
    url_addition['progress.last_visited'] = str(datetime.utcnow())

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


def set_canonical_group_to_alias(db, original_url, canonical_group):
    """If there was redirect, set the canonical group to
    the orginal alias url"""

    modification = {'canonical_group': canonical_group}
    return db['Urls'].find_one_and_update(
        {'_id': urls.hash(original_url)}, {'$set': modification})


def _update_representatives_of_canonical_groups(db, canonical_group):
    """If insertion was successful update representative of canonical group"""

    representative = select_representative_for_canonical_group(db,
                                                               canonical_group)
    return update_canonical_group_representative(db,
                                                 canonical_group,
                                                 representative)


def _format_response_header(response, url_addition):
    for key, value in response.headers.items():
        url_addition['response.' + str(key).replace('$', '')] = str(value)

    return url_addition


def set_visited_file_url(db, url, response, original_url=None):
    """Save file into database and set is as visited"""

    content_type = response.headers.get('Content-Type')

    if 'application/pdf' in content_type:
        file_type = 'pdf'
    elif 'text/plain' in content_type:
        file_type = 'txt'
    else:
        file_type = None

    url_hash = urls.hash(url)

    is_redirect, is_permanent_redirect = _determine_type_of_redirect(response)

    url_addition = {}

    # Pairing url with canonical group id
    content_hash = urls.hash_document(response.content)
    url_addition['canonical_group'] = get_or_create_canonical_group(
        db,
        content_hash)

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['indexed'] = False
    url_addition['noindex'] = False
    url_addition['file'] = True
    url_addition['file_type'] = file_type

    url_addition['progress.last_visited'] = str(datetime.utcnow())

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

    url_addition = _format_response_header(response, url_addition)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    # If there was redirect, set the canonical group to the orginal alias url
    if original_url is not None:
        set_canonical_group_to_alias(db,
                                     original_url,
                                     url_addition['canonical_group'])

    # If insertion was successful update representative of canonical group
    if result is not None:
        _update_representatives_of_canonical_groups(
            db,
            url_addition['canonical_group'])

    return result is not None


def set_visited_url(db, url, response, soup, noindex, original_url=None):
    """Try to set url to visited and update other important informations"""
    url_hash = urls.hash(url)

    is_redirect, is_permanent_redirect = _determine_type_of_redirect(response)

    url_addition = {}

    # Pairing url with canonical group id
    # Remove script tags from soup
    for script in soup('script'):
        script.extract()

    text = soup.getText(separator='\n')

    try:
        url_addition['language'] = detect(text)
    except Exception as e:
        # Fallback language
        url_addition['language'] = 'cs'

    text_hash = document.hash_document(
        document.extract_document_text_for_hash(soup))
    url_addition['canonical_group'] = get_or_create_canonical_group(db,
                                                                    text_hash)

    url_addition['visited'] = True
    url_addition['queued'] = False
    url_addition['indexed'] = False
    url_addition['noindex'] = noindex

    url_addition['progress.last_visited'] = str(datetime.utcnow())

    url_addition['content.binary'] = response.content
    url_addition['content.hashes.text'] = text_hash
    url_addition['content.encoding'] = response.encoding
    # Later detect language

    url_addition['response.elapsed'] = str(response.elapsed)
    url_addition['response.is_redirect'] = is_redirect
    url_addition['response.is_permanent_redirect'] = is_permanent_redirect
    url_addition['response.status_code'] = response.status_code
    url_addition['response.reason'] = response.reason

    url_addition = _format_response_header(response, url_addition)

    result = db['Urls'].find_one_and_update({'_id': url_hash},
                                            {'$set': url_addition})

    # If there was redirect, set the canonical group to the orginal alias url
    if original_url is not None:
        set_canonical_group_to_alias(db,
                                     original_url,
                                     url_addition['canonical_group'])

    # If insertion was successful update representative of canonical group
    if result is not None:
        _update_representatives_of_canonical_groups(
            db,
            url_addition['canonical_group'])

    return result is not None


def is_first_run(db):
    result = db['Urls'].find_one({'visited': True})

    return result is None


def reset_visited_for_fast_recrawl(db):
    result = db['Urls'].update_many(
        {'visited': True, 'alias': False, 'invalid': False},
        {'$set': {'visited': False}})

    return result is not None


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

    result = db['Urls'].find_one_and_update(
        {'_id': url_hash},
        {'$set': {
            'queued': False,
            'timeout.timeout': True,
            'timeout.last_timeout': str(datetime.utcnow())
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


def is_queued(db, url):
    """Check if url is queued"""
    result = db['Urls'].find_one({'queued': True})

    if result is not None:
        return True


def should_crawler_wait(db):
    """Check if crawler can terminate or not"""
    result = db['Urls'].find_one({'$or': [
        {'$and': [
            {'visited': False},
            {'queued': True}]},
        {'$and': [
            {'visited': False},
            {'queued': False},
            {'timeout': {'$exists': False}}]}]})

    return not ((result is None) or (len(result) == 0))


def get_crawler_stats(db):
    stats = {}

    stats['urls_count'] = db['Urls'].count()
    stats['files_count'] = db['Urls'].find({'file': True, 'invalid': False, 'alias': False}).count()
    stats['invalid_count'] = db['Urls'].find(
        {'invalid': True, 'alias': False}).count()
    stats['aliases_count'] = db['Urls'].find({'alias': True}).count()
    stats['timeout_count'] = db['Urls'].find({'timeout.timeout': True}).count()
    stats['urls_visited'] = db['Urls'].find({'visited': True}).count()
    stats['urls_queued'] = db['Urls'].find(
        {'$and': [{'visited': False}, {'queued': True}]}).count()
    stats['urls_not_queued'] = db['Urls'].find(
        {'$and': [{'visited': False},
                  {'queued': False},
                  {'timeout': {'$exists': False}}]}).count()
    stats['number_of_domains'] = get_number_of_domains(db)
    stats['number_of_servers'] = get_number_of_servers(db)

    return stats


def get_number_of_domains(db):
    return len(db['Limiter'].distinct('domain'))


def get_number_of_servers(db):
    return len(db['Limiter'].distinct('ip'))


def insert_engine_start(client, task_id, crawler_settings):
    db_stats = get_stats_database(client)
    start_time = datetime.utcnow()

    stats_object = {
        'task_id': task_id,
        'progress': {'start': start_time,
                     'end': None,
                     'result': 'running',
                     'stage': 'loading'},
        'crawler': {'result': None,
                    'start': None,
                    'end': None},
        'pagerank': {'result': None,
                     'start': None,
                     'end': None},
        'indexer': {'result': None,
                    'start': None,
                    'end': None},
        'limit_domain': crawler_settings.get('limit_domain'),
    }

    return db_stats['Stats'].insert_one(stats_object)


def insert_engine_finish(client, task_id, reason):
    db_stats = get_stats_database(client)
    end_time = datetime.utcnow()

    return db_stats['Stats'].find_one_and_update(
        {'task_id': task_id},
        {'$set': {'progress.end': end_time,
                  'progress.result': reason}})


def insert_sub_task_start(client, task_id, subtask_name):
    db_stats = get_stats_database(client)
    start_time = datetime.utcnow()

    return db_stats['Stats'].find_one_and_update(
        {'task_id': task_id},
        {'$set': {subtask_name + '.start': start_time,
                  subtask_name + '.result': "running",
                  'progress.stage': subtask_name}})


def insert_sub_task_finish(client, task_id, subtask_name, reason):
    db_stats = get_stats_database(client)
    end_time = datetime.utcnow()

    return db_stats['Stats'].find_one_and_update(
        {'task_id': task_id},
        {'$set': {subtask_name + '.end': end_time,
                  subtask_name + '.result': reason}})


def update_crawler_progress(client, db, task_id):
    db_stats = get_stats_database(client)

    stats = get_crawler_stats(db)

    return db_stats['Stats'].find_one_and_update(
        {'task_id': task_id},
        {'$set': {'crawler.progress': stats}})


def update_pagerank_progress(client, task_id, stage):
    db_stats = get_stats_database(client)
    start_time = datetime.utcnow()

    return db_stats['Stats'].find_one_and_update(
        {'task_id': task_id},
        {'$set': {'pagerank.progress.' + stage: start_time}})


def update_indexer_progress(client, task_id, progress):
    db_stats = get_stats_database(client)

    actual = db_stats['Stats'].find_one({'task_id': task_id})

    if actual is None:
        return

    indexer_progress = actual.get('indexer').get('progress')

    if indexer_progress is None:
        new = int(progress)
    else:
        new = int(indexer_progress.get('progress')) + int(progress)

    return db_stats['Stats'].find_one_and_update(
        {'task_id': task_id},
        {'$set': {'indexer.progress.progress': new}})


def get_latest_stats(client):
    db_stats = get_stats_database(client)

    aware_times = db_stats['Stats'].with_options(codec_options=CodecOptions(
        tz_aware=True,
        tzinfo=pytz.timezone('Europe/Prague')))

    result = aware_times.find().sort('$natural', pymongo.DESCENDING).limit(1)

    if result.count() == 0:
        return None
    else:
        return result[0]


def insert_or_iterate_search_words(db, words):
    for word in words:
        try:
            db['SearchWordsStats'].insert({'word': word, 'count': 0})
        except Exception as e:
            pass

        db['SearchWordsStats'].update({'word': word}, {'$inc': {'count': 1}})


def insert_search_query(db, query, language):
    db['SearchStats'].insert(
        {'query': query, 'language': language, 'date': str(datetime.utcnow())})


def get_count_of_not_indexed(db):
    count = db['Urls'].find({
        'page.visited': True,
        'page.noindex': False,
        'page.file': False,  # Just for now
        'page.invalid': False,
        'page.response.status_code': 200,
        'page.indexed': False
    }).count()

    return count


# DEPRECATED
def get_batch_for_indexer(db, size):
    pipeline = [
        {'$lookup': {
            'from': 'Urls',
            'localField': 'representative',
            'foreignField': '_id',
            'as': 'page'
        }},
        {'$unwind': '$page'},
        {'$match': {
            'page.visited': True,
            'page.noindex': False,
            'page.file': False,  # Just for now
            'page.invalid': False,
            'page.response.status_code': 200,
            'page.indexed': False
        }},
        {'$project': {'representative': 1,
                      'page.url': 1,
                      'page.depth': 1,
                      'page.file': 1,
                      'page.language': 1,
                      'page.content.binary': 1,
                      'page.pagerank': 1}},
        {'$limit': size}
    ]

    url_batch = db['CanonicalGroups'].aggregate(
        pipeline, allowDiskUse=True)

    return url_batch


def get_batch_of_ids_for_indexer(db, size):
    pipeline = [
        {'$lookup': {
            'from': 'Urls',
            'localField': 'representative',
            'foreignField': '_id',
            'as': 'page'
        }},
        {'$unwind': '$page'},
        {'$match': {
            'page.visited': True,
            'page.noindex': False,
            'page.invalid': False,
            'page.response.status_code': 200,
            'page.indexed': False
        }},
        {'$project': {'representative': 1}},
        {'$limit': size}
    ]

    url_batch = db['CanonicalGroups'].aggregate(
        pipeline, allowDiskUse=True)

    return url_batch


def set_documents_as_indexed(db, document_hashes):
    requests = []

    for url_hash in document_hashes:
        requests.append(pymongo.UpdateOne(
            {'_id': url_hash}, {'$set': {'indexed': True}}))

    return db['Urls'].bulk_write(requests)
