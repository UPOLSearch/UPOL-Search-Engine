from upol_search_engine.celery_app import app


# @app.task(queue='search_engine_sub_tasks', bind=True)
def indexer_task(crawler_settings, indexer_settings):
    from datetime import datetime
    from upol_search_engine.db import mongodb
    from upol_search_engine.db import postgresql
    from upol_search_engine.upol_indexer import indexer
    import locale

    locale.setlocale(locale.LC_ALL, 'cs_CZ.utf-8')

    start_time = datetime.now()

    # self.update_state(state='STARTING', meta={'start': start_time})

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

    batch_number = 0

    while True:
        # self.update_state(state='RUNNING', meta={'start': start_time,
        #                                          'batch_number': batch_number})

        document_batch = mongodb.get_batch_for_indexer(mongodb_database,
                                                       mongodb_batch_size)

        if document_batch is None:
            break

        batch_number += 1
        indexed_rows = []
        document_hashes = []

        for document in document_batch:
            row = indexer.prepare_one_document_for_index(document)
            if row is not None:
                indexed_rows.append(row)
                document_hashes.append(document.get('representative'))

        if len(indexed_rows) > 0:
            postgresql.insert_rows_into_index(postgresql_client,
                                              postgresql_cursor,
                                              indexed_rows,
                                              postgresql_table_name)
            mongodb.set_documents_as_indexed(mongodb_database, document_hashes)
        else:
            break

    postgresql.change_table_to_production(postgresql_client,
                                          postgresql_cursor,
                                          postgresql_table_name,
                                          postgresql_table_name_production)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()

    # self.update_state(state='DONE', meta={'start': start_time,
    #                                       'end': datetime.now()})
