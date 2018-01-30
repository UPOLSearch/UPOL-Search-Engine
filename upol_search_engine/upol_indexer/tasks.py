from upol_search_engine.celery_app import app


def indexer_task(crawler_settings, indexer_settings, task_id):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    from celery import group
    from celery.result import allow_join_result
    import locale

    locale.setlocale(locale.LC_ALL, 'cs_CZ.utf-8')

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(
        crawler_settings.get('limit_domain'), mongodb_client)
    mongodb_batch_size = indexer_settings.get('batch_size')

    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()
    postgresql_table_name = indexer_settings.get('table_name')
    postgresql_table_name_production = indexer_settings.get('table_name_production')

    # Test if postgresql table is ready
    if not postgresql.test_if_table_exists(postgresql_cursor, postgresql_table_name):
        if not postgresql.test_if_table_exists(postgresql_cursor,
                                               postgresql_table_name_production):
            postgresql.create_function(postgresql_client,
                                       postgresql_cursor)

        postgresql.reset_and_init_db(postgresql_client,
                                     postgresql_cursor,
                                     postgresql_table_name)

    tasks_list = []

    while True:
        document_batch = mongodb.get_batch_of_ids_for_indexer(mongodb_database,
                                                              mongodb_batch_size)

        document_batch = list(document_batch)

        if len(document_batch) == 0:
            break

        document_ids = []

        for document in document_batch:
            document_ids.append(document.get('representative'))

        if len(document_ids) > 0:
            mongodb.set_documents_as_indexed(mongodb_database, document_ids)
            tasks_list.append(index_batch_task.s(document_ids,
                                                 task_id,
                                                 crawler_settings,
                                                 indexer_settings))

    tasks_group = group(*tasks_list)

    with allow_join_result():
        result = tasks_group.apply_async()
        result.join()

    postgresql.change_table_to_production(postgresql_client,
                                          postgresql_cursor,
                                          postgresql_table_name,
                                          postgresql_table_name_production)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()


@app.task(queue='indexer')
def index_batch_task(ids_batch, task_id, crawler_settings, indexer_settings):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    from upol_search_engine.upol_indexer import indexer
    from celery.utils.log import get_task_logger

    log = get_task_logger(__name__)

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(
        crawler_settings.get('limit_domain'), mongodb_client)

    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()
    postgresql_table_name = indexer_settings.get('table_name')
    postgresql_table_name_production = indexer_settings.get('table_name_production')

    batch = mongodb.get_batch_by_id(mongodb_database, ids_batch)

    indexed_rows = []
    copied_rows = []

    does_production_exists = postgresql.test_if_table_exists(
        postgresql_cursor, postgresql_table_name_production)

    for document in batch:
        is_file = document.get('file')

        if does_production_exists:
            url_hash = document.get('_id')
            content_hash = document.get('content').get('hashes').get('text')

            production_document = postgresql.get_document_by_hash(postgresql_client,
                                                                  postgresql_cursor,
                                                                  url_hash,
                                                                  postgresql_table_name_production)
        else:
            production_document = None

        if (production_document is None) or (production_document[10] != content_hash):
            log.info('INDEXER: Indexing document.')

            if is_file:
                try:
                    row = indexer.prepare_one_file_for_index(
                        document, crawler_settings.get('limit_domain'))
                except Exception as e:
                    log.exception('Exception: {0}'.format(document.get('url')))
                    log.exception('Exception description: {0}'.format(e))
                    row = None
            else:
                row = indexer.prepare_one_document_for_index(
                    document, crawler_settings.get('limit_domain'))

            if row is not None:
                indexed_rows.append(row)
        else:
            log.info('INDEXER: Coping document.')

            copied_rows.append(production_document)

            postgresql.copy_row_from_table_to_table(
                postgresql_client,
                postgresql_cursor,
                url_hash,
                postgresql_table_name_production,
                postgresql_table_name)

    if len(indexed_rows) > 0:
            postgresql.insert_rows_into_index(postgresql_client,
                                              postgresql_cursor,
                                              indexed_rows,
                                              postgresql_table_name)

    mongodb.update_indexer_progress(
        mongodb_client, task_id, len(indexed_rows) + len(copied_rows))

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()
