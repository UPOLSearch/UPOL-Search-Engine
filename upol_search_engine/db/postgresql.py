import psycopg2
from psycopg2 import sql
from upol_search_engine import settings


def create_client():
    postgresql_client = psycopg2.connect(
        dbname=settings.CONFIG.get('General', 'postgresql_db_name'),
        user=settings.CONFIG.get('General', 'postgresql_db_user'),
        password=settings.CONFIG.get('General', 'postgresql_db_password'),
        host=settings.CONFIG.get('General', 'postgresql_db_server'),
        port=settings.CONFIG.get('General', 'postgresql_db_port'))

    postgresql_client.autocommit = True

    return postgresql_client


def create_function(postgresql_client, postgresql_cursor):
    postgresql_cursor.execute(
        "DROP FUNCTION IF EXISTS documents_search_trigger() CASCADE;")

    postgresql_cursor.execute(
        """CREATE FUNCTION documents_search_trigger() RETURNS trigger AS $$
        begin
          new.search_index :=
            setweight(to_tsvector('czech', coalesce(new.description, '')), 'B') ||
            setweight(to_tsvector('czech', coalesce(new.keywords, '')), 'A') ||
            setweight(to_tsvector('czech', coalesce(new.important_headlines, '')), 'B') ||
            setweight(to_tsvector('czech', coalesce(new.content, '')), 'C') ||
            setweight(to_tsvector('czech', coalesce(new.title, '')), 'A') ||
            setweight(to_tsvector('czech', coalesce(new.url_words, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(new.description, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(new.keywords, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(new.important_headlines, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(new.content, '')), 'C') ||
            setweight(to_tsvector('english', coalesce(new.title, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(new.url_words, '')), 'A')
            ;
          return new;
        end
        $$ LANGUAGE plpgsql;""")

    # Microformat

    postgresql_cursor.execute(
        "DROP FUNCTION IF EXISTS microformat_search_trigger() CASCADE;")

    postgresql_cursor.execute(
        """CREATE FUNCTION microformat_search_trigger() RETURNS trigger AS $$
        begin
          new.microformat_index :=
            setweight(to_tsvector('czech', new.json), 'A');
          return new;
        end
        $$ LANGUAGE plpgsql;""")

    postgresql_client.commit()


def reset_and_init_languages(postgresql_client, postgresql_cursor):
    sql_for_execute = []
    sql_for_execute.append(sql.SQL('DROP EXTENSION IF EXISTS unaccent CASCADE;'))
    sql_for_execute.append(sql.SQL('CREATE EXTENSION unaccent;'))

    sql_for_execute.append(sql.SQL('DROP TEXT SEARCH CONFIGURATION IF EXISTS public.czech;'))
    sql_for_execute.append(sql.SQL('CREATE TEXT SEARCH CONFIGURATION public.czech ( COPY = pg_catalog.simple ) ;'))

    sql_for_execute.append(sql.SQL('DROP TEXT SEARCH DICTIONARY IF EXISTS czech_ispell;'))
    sql_for_execute.append(sql.SQL("""CREATE TEXT SEARCH DICTIONARY czech_ispell (
                                   TEMPLATE  = ispell,
                                   DictFile  = cs_cz,
                                   AffFile   = cs_cz,
                                   StopWords = czech
                                   );"""))

    sql_for_execute.append(
        sql.SQL('ALTER TEXT SEARCH CONFIGURATION czech ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part WITH unaccent, czech_ispell, simple;'))


    sql_for_execute.append(sql.SQL('DROP TEXT SEARCH CONFIGURATION IF EXISTS public.english;'))
    sql_for_execute.append(sql.SQL('CREATE TEXT SEARCH CONFIGURATION public.english ( COPY = pg_catalog.simple ) ;'))

    sql_for_execute.append(sql.SQL('DROP TEXT SEARCH DICTIONARY IF EXISTS english_ispell;'))
    sql_for_execute.append(sql.SQL("""CREATE TEXT SEARCH DICTIONARY english_ispell (
                                   TEMPLATE  = ispell,
                                   DictFile  = en_us,
                                   AffFile   = en_us,
                                   StopWords = english
                                   );"""))

    sql_for_execute.append(
        sql.SQL('ALTER TEXT SEARCH CONFIGURATION english ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part WITH unaccent, english_ispell, english_stem;'))

    for s in sql_for_execute:
        postgresql_cursor.execute(s)

    postgresql_client.commit()


def create_psql_index(postgresql_client, postgresql_cursor,
                      table_name, table_field, index_name):
    postgresql_cursor.execute(
        sql.SQL(
            "CREATE INDEX {} ON {} USING gin({});").format(
                sql.Identifier(index_name),
                sql.Identifier(table_name),
                sql.Identifier(table_field)))

    postgresql_client.commit()


def reset_and_init_db(postgresql_client, postgresql_cursor,
                      table_name, microformat_table_name):
    postgresql_cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))

    postgresql_cursor.execute(sql.SQL(
        """CREATE TABLE {} (hash varchar PRIMARY KEY,
        url text,
        url_decoded text,
        url_words text,
        title text,
        language text,
        keywords text,
        description text,
        important_headlines text,
        content text,
        content_hash varchar,
        depth integer,
        is_file boolean,
        file_type text,
        pagerank double precision,
        url_length integer,
        search_index tsvector);""").format(sql.Identifier(table_name)))

    postgresql_cursor.execute(
        sql.SQL("""CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE ON {} FOR EACH ROW EXECUTE PROCEDURE documents_search_trigger();""").format(sql.Identifier(table_name)))

    postgresql_cursor.execute(
        sql.SQL("ALTER TABLE {} ENABLE TRIGGER tsvectorupdate;").format(
            sql.Identifier(table_name)))

    # postgresql_cursor.execute(sql.SQL("DROP INDEX IF EXISTS search_idx;"))

    # try:
    #     postgresql_cursor.execute(
    #         sql.SQL(
    #             "CREATE INDEX search_idx ON {} USING gin(search_index);").format(
    #                 sql.Identifier(table_name)))
    # except Exception as e:
    #     pass

    # Microformat

    postgresql_cursor.execute(sql.SQL("DROP TABLE IF EXISTS {0};").format(
        sql.Identifier(microformat_table_name)))

    postgresql_cursor.execute(sql.SQL(
        """CREATE TABLE {} (hash varchar PRIMARY KEY,
        json jsonb,
        type varchar,
        microformat_index tsvector);""").format(
            sql.Identifier(microformat_table_name)))

    postgresql_cursor.execute(
        sql.SQL("CREATE TRIGGER microformat_tsvectorupdate BEFORE INSERT OR UPDATE ON {} FOR EACH ROW EXECUTE PROCEDURE microformat_search_trigger();").format(
            sql.Identifier(microformat_table_name)))

    postgresql_cursor.execute(
        sql.SQL("ALTER TABLE {} ENABLE TRIGGER microformat_tsvectorupdate;").format(
            sql.Identifier(microformat_table_name)))

    # postgresql_cursor.execute(sql.SQL("DROP INDEX IF EXISTS microformat_idx;"))

    # try:
    #     postgresql_cursor.execute(
    #         sql.SQL(
    #             "CREATE INDEX microformat_idx ON {} USING gin(microformat_index);").format(
    #                 sql.Identifier(microformat_table_name)))
    # except Exception as e:
    #     pass


    postgresql_client.commit()


def get_document_by_hash(psql_client, psql_cursor, url_hash, table_name):
    psql_cursor.execute(sql.SQL("SELECT * FROM {} WHERE hash = {};").format(
        sql.Identifier(table_name), sql.Literal(url_hash)))

    result = psql_cursor.fetchone()
    psql_client.commit()

    return result


def change_table_to_production(postgresql_client, postgresql_cursor,
                               table_name, table_name_production):
    if test_if_table_exists(postgresql_client, postgresql_cursor, table_name_production):
        postgresql_cursor.execute(
            sql.SQL("ALTER table {} RENAME TO {};").format(
                sql.Identifier(table_name_production), sql.Identifier("tmp")))

    postgresql_cursor.execute(
        sql.SQL("ALTER table {} RENAME TO {};").format(
            sql.Identifier(table_name), sql.Identifier(table_name_production)))

    if test_if_table_exists(postgresql_client, postgresql_cursor, "tmp"):
        postgresql_cursor.execute("DROP TABLE tmp;")

    postgresql_client.commit()


def insert_rows_into_index(psql_client, psql_cursor, indexed_rows, table_name):
    dataText = ','.join(
        psql_cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', row).decode('utf-8') for row in indexed_rows)

    response = psql_cursor.execute(
        'INSERT INTO {0} VALUES {1} ON CONFLICT DO NOTHING;'.format(table_name, dataText))

    psql_client.commit()

    return response


def insert_microformat(psql_client, psql_cursor, json, json_hash, metadata_type, table_name):
    psql_cursor.execute(sql.SQL("INSERT INTO {} (hash, json, type) VALUES ({}, {}, {});").format(
        sql.Identifier(table_name), sql.Literal(json_hash), sql.Literal(json), sql.Literal(metadata_type)))

    psql_client.commit()


def copy_row_from_table_to_table(psql_client, psql_cursor, document_hash, table_from, table_to):
    psql_cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM {} WHERE hash = {}").format(
        sql.Identifier(table_to), sql.Identifier(table_from), sql.Literal(document_hash)))

    psql_client.commit()


def test_if_table_exists(psql_client, psql_cursor, table_name):
    psql_cursor.execute(
        'SELECT exists(SELECT * FROM information_schema.tables WHERE table_name=\'{0}\');'.format(table_name))

    result = bool(psql_cursor.fetchone()[0])

    psql_client.commit()

    return result


# def get_ts_vector_from_text(psql_client, psql_cursor, language, text):
#     psql_cursor.execute(
#         sql.SQL("SELECT to_tsvector({}, {});").format(
#             sql.Literal(language), sql.Literal(text))
#     )
#
#     result = psql_cursor.fetchone()[0]
#
#     psql_client.commit()
#
#     return result
