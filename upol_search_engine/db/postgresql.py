import psycopg2
from upol_search_engine import settings


def create_client():
    postgresql_client = psycopg2.connect(
        dbname=settings.CONFIG.get('Settings', 'postgresql_db_name'),
        user=settings.CONFIG.get('Settings', 'postgresql_db_user'),
        password=settings.CONFIG.get('Settings', 'postgresql_db_password'),
        host=settings.CONFIG.get('Settings', 'postgresql_db_server'),
        port=settings.CONFIG.get('Settings', 'postgresql_db_port'))

    return postgresql_client


def reset_and_init_db(postgresql_client, postgresql_cursor, table_name):
    postgresql_cursor.execute("DROP TABLE IF EXISTS {0};".format(table_name))
    postgresql_cursor.execute(
        "DROP TRIGGER IF EXISTS tsvectorupdate ON {0};".format(table_name))
    postgresql_cursor.execute(
        "DROP FUNCTION IF EXISTS documents_search_trigger();")
    postgresql_cursor.execute(
        """CREATE TABLE {0} (hash varchar PRIMARY KEY,
        url text,
        url_decoded text,
        title text,
        language text,
        keywords text,
        description text,
        content text,
        depth integer,
        is_file boolean,
        pagerank double precision,
        url_length integer,
        search_czech_index tsvector);""".format(table_name))

    postgresql_cursor.execute(
        """CREATE FUNCTION documents_search_trigger() RETURNS trigger AS $$
        begin
          new.search_czech_index :=
            setweight(to_tsvector('czech', coalesce(new.description, '')), 'B') ||
            setweight(to_tsvector('czech', coalesce(new.keywords, '')), 'A') ||
            setweight(to_tsvector('czech', coalesce(new.content, '')), 'B')||
            setweight(to_tsvector('czech', coalesce(new.title, '')), 'A');
          return new;
        end
        $$ LANGUAGE plpgsql;""")

    postgresql_cursor.execute(
        """CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE ON {0} FOR EACH ROW EXECUTE PROCEDURE documents_search_trigger();""".format(table_name))

    postgresql_cursor.execute(
        "ALTER TABLE {0} ENABLE TRIGGER tsvectorupdate;".format(table_name))

    postgresql_client.commit()


def change_table_to_production(postgresql_client, postgresql_cursor,
                               table_name, table_name_production):
    if test_if_table_exists(postgresql_cursor, table_name):
        postgresql_cursor.execute("ALTER table %s RENAME TO %s;",
                                  (table_name_production, "tmp"))

    postgresql_cursor.execute("ALTER table %s RENAME TO %s;",
                              (table_name, table_name_production))

    postgresql_cursor.execute("DROP TABLE tmp;")

    postgresql_client.commit()


def insert_rows_into_index(psql_client, psql_cursor, indexed_rows, table_name):
    dataText = ','.join(
        psql_cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', row).decode('utf-8') for row in indexed_rows)

    response = psql_cursor.execute(
        'INSERT INTO {0} VALUES {1} ON CONFLICT DO NOTHING;'.format(table_name, dataText))

    psql_client.commit()

    return response


def test_if_table_exists(psql_cursor, table_name):
    psql_cursor.execute(
        'SELECT exists(SELECT * FROM information_schema.tables WHERE table_name=\'{0}\');'.format(table_name))

    return bool(psql_cursor.fetchone()[0])
