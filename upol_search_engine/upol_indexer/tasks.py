from upol_search_engine import settings
from upol_search_engine.celery_app import app


def indexer_task(crawler_settings, indexer_settings, task_id):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    import locale
    from celery.result import AsyncResult
    from celery.states import PENDING, STARTED, RECEIVED, SUCCESS
    import time

    locale.setlocale(locale.LC_ALL, 'cs_CZ.utf-8')

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(
        crawler_settings.get('limit_domain'), mongodb_client)
    mongodb_batch_size = indexer_settings.get('batch_size')

    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()
    postgresql_table_name = indexer_settings.get('table_name')
    postgresql_table_name_production = indexer_settings.get(
        'table_name_production')
    postgresql_metadata_table_name = indexer_settings.get('metadata_table_name')
    postgresql_metadata_table_name_production = indexer_settings.get(
        'metadatatable_name_production')

    # Test if postgresql table is ready
    # if (not postgresql.test_if_table_exists(postgresql_client, postgresql_cursor, postgresql_table_name)) or (not postgresql.test_if_table_exists(postgresql_client, postgresql_cursor, 'metadata_tmp')):
    postgresql.reset_and_init_db(postgresql_client,
                                 postgresql_cursor,
                                 postgresql_table_name,
                                 postgresql_metadata_table_name)

    tasks_list = []

    while True:
        document_batch = mongodb.get_batch_of_ids_for_indexer(
            mongodb_database,
            mongodb_batch_size)

        document_batch = list(document_batch)

        if len(document_batch) == 0:
            break

        document_ids = []

        for document in document_batch:
            document_ids.append(document.get('representative'))

        if len(document_ids) > 0:
            mongodb.set_documents_as_indexed(mongodb_database, document_ids)
            counter = 0
            for document_id in document_ids:
                counter += 1
                tasks_list.append(index_document_task.delay(document_id,
                                                            task_id,
                                                            crawler_settings,
                                                            indexer_settings))

    waiting = True

    while waiting:
        n_of_running = 0

        for task in tasks_list:
            state = AsyncResult(task.task_id).status

            if state == PENDING or state == STARTED or state == RECEIVED:
                n_of_running += 1

        if n_of_running == 0:
            waiting = False
            for task in tasks_list:
                state = AsyncResult(task.task_id).status
                if state != SUCCESS:
                    print(state)

        time.sleep(10)

    postgresql.change_table_to_production(postgresql_client,
                                          postgresql_cursor,
                                          postgresql_table_name,
                                          postgresql_table_name_production)

    postgresql.change_table_to_production(postgresql_client,
                                          postgresql_cursor,
                                          postgresql_metadata_table_name,
                                          postgresql_metadata_table_name_production)

    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()


@app.task(rate_limit=settings.CONFIG.get('Indexer', 'indexer_task_frequency'),
          queue='indexer',
          task_compression='zlib')
def index_document_task(document_id, task_id,
                        crawler_settings, indexer_settings):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    from upol_search_engine.upol_indexer import indexer
    from celery.utils.log import get_task_logger
    from upol_search_engine.upol_indexer import microformat
    from psycopg2 import IntegrityError
    import json
    import hashlib

    log = get_task_logger(__name__)

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(
        crawler_settings.get('limit_domain'), mongodb_client)
    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()
    postgresql_table_name = indexer_settings.get('table_name')
    postgresql_table_name_production = indexer_settings.get(
        'table_name_production')
    postgresql_metadata_table_name = indexer_settings.get('metadata_table_name')
    postgresql_metadata_table_name_production = indexer_settings.get(
        'metadatatable_name_production')

    try:
        document = mongodb.get_document_by_id(mongodb_database, document_id)

        indexed_rows = []
        copied_rows = []

        does_production_exists = postgresql.test_if_table_exists(
            postgresql_client,
            postgresql_cursor,
            postgresql_table_name_production)

        try:
            is_file = document.get('file')

            # Metadata
            if not is_file:
                soup = indexer.get_soup_from_document(document)
                metadata = microformat.find_microformat_on_page(soup)

                if metadata is not None:
                    parsed_metadata = microformat.parse_json(metadata)
                    metadata_hash = hashlib.sha1(
                        str(parsed_metadata).encode('utf-8')).hexdigest()

                    if microformat.validate_json_schema(parsed_metadata):

                        parsed_metadata, metadata_type = microformat.prepare_metadata_for_insert(parsed_metadata)

                        try:
                            postgresql.insert_microformat(postgresql_client,
                                                          postgresql_cursor,
                                                          json.dumps(parsed_metadata),
                                                          metadata_hash,
                                                          metadata_type,
                                                          postgresql_metadata_table_name)
                        except IntegrityError as e:
                            log.info('METADATA duplicity: {}'.format(
                                parsed_metadata))
                    else:
                        log.info('METADATA not valid: {}'.format(
                            document.get('url')))

            if does_production_exists:
                url_hash = document.get('_id')
                content_hash = document.get('content').get('hashes').get('text')

                production_document = postgresql.get_document_by_hash(
                    postgresql_client,
                    postgresql_cursor,
                    url_hash,
                    postgresql_table_name_production)
            else:
                production_document = None

            if (production_document is None) or (production_document[10] != content_hash):
                if is_file:
                    log.info('INDEXER: Indexing document (file).')

                    # Index only pdf this time
                    if document.get('file_type') == 'pdf':
                        try:
                            row = indexer.prepare_one_file_for_index(
                                document, crawler_settings.get('limit_domain'))
                        except Exception as e:
                            log.exception('Exception: {0}'.format(document.get('url')))
                            row = None
                    else:
                        row = None
                else:
                    log.info('INDEXER: Indexing document.')
                    row = indexer.prepare_one_document_for_index(
                        document,
                        soup,
                        crawler_settings.get('limit_domain'))

                if row is not None:
                    indexed_rows.append(row)
            else:
                if is_file:
                    log.info('INDEXER: Coping document (file).')
                else:
                    log.info('INDEXER: Coping document.')

                copied_rows.append(production_document)

                postgresql.copy_row_from_table_to_table(
                    postgresql_client,
                    postgresql_cursor,
                    url_hash,
                    postgresql_table_name_production,
                    postgresql_table_name)
        except Exception as e:
            log.exception('Exception: {0}'.format(document.get('url')))

        if len(indexed_rows) > 0:
                postgresql.insert_rows_into_index(postgresql_client,
                                                  postgresql_cursor,
                                                  indexed_rows,
                                                  postgresql_table_name)

        mongodb.update_indexer_progress(
            mongodb_client, task_id, len(indexed_rows) + len(copied_rows))
    except Exception as e:
        log.exception('Exception: INDEXER TASK POSSIBLE FAILURE')
    finally:
        postgresql_cursor.close()
        postgresql_client.close()
        mongodb_client.close()


# @app.task(queue='indexer', task_compression='zlib')
# def index_batch_task(ids_batch, task_id, crawler_settings, indexer_settings):
#     from upol_search_engine.db import mongodb
#     from upol_search_engine.db import postgresql
#     from upol_search_engine.upol_indexer import indexer
#     from celery.utils.log import get_task_logger
#
#     log = get_task_logger(__name__)
#
#     mongodb_client = mongodb.create_client()
#     mongodb_database = mongodb.get_database(
#         crawler_settings.get('limit_domain'), mongodb_client)
#     postgresql_client = postgresql.create_client()
#     postgresql_cursor = postgresql_client.cursor()
#     postgresql_table_name = indexer_settings.get('table_name')
#     postgresql_table_name_production = indexer_settings.get('table_name_production')
#
#     try:
#         batch = mongodb.get_batch_by_id(mongodb_database, ids_batch)
#
#         indexed_rows = []
#         copied_rows = []
#
#         does_production_exists = postgresql.test_if_table_exists(
#             postgresql_client, postgresql_cursor, postgresql_table_name_production)
#
#         for document in batch:
#             try:
#                 is_file = document.get('file')
#
#                 if does_production_exists:
#                     url_hash = document.get('_id')
#                     content_hash = document.get('content').get('hashes').get('text')
#
#                     production_document = postgresql.get_document_by_hash(postgresql_client,
#                                                                           postgresql_cursor,
#                                                                           url_hash,
#                                                                           postgresql_table_name_production)
#                 else:
#                     production_document = None
#
#                 if (production_document is None) or (production_document[10] != content_hash):
#                     if is_file:
#                         log.info('INDEXER: Indexing document (file).')
#
#                         # Index only pdf this time
#                         if document.get('file_type') == 'pdf':
#                             try:
#                                 row = indexer.prepare_one_file_for_index(
#                                     document, crawler_settings.get('limit_domain'))
#                             except Exception as e:
#                                 log.exception('Exception: {0}'.format(document.get('url')))
#                                 row = None
#                         else:
#                             row = None
#                     else:
#                         log.info('INDEXER: Indexing document.')
#                         row = indexer.prepare_one_document_for_index(
#                             document, crawler_settings.get('limit_domain'))
#
#                     if row is not None:
#                         indexed_rows.append(row)
#                 else:
#                     if is_file:
#                         log.info('INDEXER: Coping document (file).')
#                     else:
#                         log.info('INDEXER: Coping document.')
#
#                     copied_rows.append(production_document)
#
#                     postgresql.copy_row_from_table_to_table(
#                         postgresql_client,
#                         postgresql_cursor,
#                         url_hash,
#                         postgresql_table_name_production,
#                         postgresql_table_name)
#             except Exception as e:
#                 log.exception('Exception: {0}'.format(document.get('url')))
#
#         if len(indexed_rows) > 0:
#                 postgresql.insert_rows_into_index(postgresql_client,
#                                                   postgresql_cursor,
#                                                   indexed_rows,
#                                                   postgresql_table_name)
#
#         mongodb.update_indexer_progress(
#             mongodb_client, task_id, len(indexed_rows) + len(copied_rows))
#     except Exception as e:
#         log.exception('Exception: INDEXER TASK POSSIBLE FAILURE')
#     finally:
#         postgresql_cursor.close()
#         postgresql_client.close()
#         mongodb_client.close()
