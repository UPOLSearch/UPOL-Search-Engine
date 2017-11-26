from celery import group
from celery.result import allow_join_result
from upol_search_engine.celery_app import app


@app.task(queue='indexer')
def page_indexer_task(crawler_settings, indexer_settings):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    from upol_search_engine.upol_indexer import indexer

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(
        crawler_settings.get('limit_domain'), mongodb_client)
    mongodb_batch_size = indexer_settings.get('batch_size')

    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()
    postgresql_table_name = indexer_settings.get('table_name')

    batch_number = 0

    progress_pages = 0

    while True:
        document_batch = mongodb.get_batch_for_indexer(mongodb_database,
                                                       mongodb_batch_size)

        document_batch = list(document_batch)

        if len(document_batch) == 0:
            break

        batch_number += 1
        indexed_rows = []
        document_hashes = []

        for document in document_batch:
            row = indexer.prepare_one_document_for_index(
                document, crawler_settings.get('limit_domain'))

            document_hashes.append(document.get('representative'))

            if row is not None:
                indexed_rows.append(row)

        if len(indexed_rows) > 0:
            postgresql.insert_rows_into_index(postgresql_client,
                                              postgresql_cursor,
                                              indexed_rows,
                                              postgresql_table_name)

        if len(document_hashes) > 0:
            mongodb.set_documents_as_indexed(mongodb_database, document_hashes)
            progress_pages = progress_pages + len(indexed_rows)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()


@app.task(queue='indexer')
def file_indexer_task(crawler_settings, indexer_settings):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    from upol_search_engine.upol_indexer import indexer

    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database(
        crawler_settings.get('limit_domain'), mongodb_client)
    mongodb_batch_size = indexer_settings.get('batch_size')

    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()
    postgresql_table_name = indexer_settings.get('table_name')

    batch_number = 0

    progress_pages = 0

    while True:
        document_batch = mongodb.get_batch_of_files_for_indexer(mongodb_database,
                                                                mongodb_batch_size)

        document_batch = list(document_batch)

        if len(document_batch) == 0:
            break

        batch_number += 1
        indexed_rows = []
        document_hashes = []

        for document in document_batch:
            row = indexer.prepare_one_file_for_index(
                document, crawler_settings.get('limit_domain'))

            document_hashes.append(document.get('representative'))

            if row is not None:
                indexed_rows.append(row)

        if len(indexed_rows) > 0:
            postgresql.insert_rows_into_index(postgresql_client,
                                              postgresql_cursor,
                                              indexed_rows,
                                              postgresql_table_name)

        if len(document_hashes) > 0:
            mongodb.set_documents_as_indexed(mongodb_database, document_hashes)
            progress_pages = progress_pages + len(indexed_rows)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()


# @app.task(queue='search_engine_sub_tasks', bind=True)
def indexer_task(crawler_settings, indexer_settings, task_id):
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    import locale

    locale.setlocale(locale.LC_ALL, 'cs_CZ.utf-8')

    mongodb_client = mongodb.create_client()

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

    job = group([
        page_indexer_task.s(crawler_settings, indexer_settings),
        file_indexer_task.s(crawler_settings, indexer_settings)
    ])

    with allow_join_result():
        result = job.apply_async()
        result.join()

    # mongodb.update_indexer_progress(
    #     mongodb_client, task_id, progress_pages, total_pages)

    postgresql.change_table_to_production(postgresql_client,
                                          postgresql_cursor,
                                          postgresql_table_name,
                                          postgresql_table_name_production)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()
