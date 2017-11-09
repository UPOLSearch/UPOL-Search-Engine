from upol_search_engine.celery_app import app


def get_words_from_psql_vector(vector):
    words = []

    for element in list(filter(None, vector.split("'"))):
        if element[0] != ':':
            words.append(element)

    return words


@app.task(queue='search')
def process_search_query(query, language):
    from upol_search_engine.db import mongodb, postgresql
    
    mongodb_client = mongodb.create_client()
    mongodb_database = mongodb.get_database('stats', mongodb_client)

    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()

    ts_vector = postgresql.get_ts_vector_from_text(
        postgresql_cursor, language, query)
    words = get_words_from_psql_vector(ts_vector)

    mongodb.insert_or_iterate_search_words(mongodb_database, words)
    mongodb.insert_search_query(mongodb_database, query, language)

    postgresql_cursor.close()
    postgresql_client.close()
    mongodb_client.close()
