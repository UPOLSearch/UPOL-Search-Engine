import datetime

from flask import Blueprint, render_template, request
from langdetect import detect
from psycopg2 import sql
from upol_search_engine import settings, upol_search_engine
from upol_search_engine.upol_search_engine import tasks

mod = Blueprint('search', __name__, url_prefix='/')


@mod.route('/',  methods=['GET', 'POST'])
def home():
    TABLE_NAME = settings.CONFIG.get('Settings', 'postgresql_table_name')

    if request.method == 'GET':
        search = request.args.get('search')
        psql_client = upol_search_engine.get_db()
        psql_cursor = psql_client.cursor()

        if search is None:
            query = sql.SQL("SELECT reltuples::bigint AS estimate \
                            FROM pg_class where relname={relname};")

            psql_cursor.execute(
                query.format(relname=sql.Literal(TABLE_NAME)))
            index_size = psql_cursor.fetchall()[0][0]
            index_size = '{:,}'.format(index_size).replace(',', ' ')

            return render_template('search/home.html', index_size=index_size)
        else:
            page = request.args.get('page')
            search_language = detect(search)

            if page is None:
                page = 1
            else:
                page = int(page)

            start_time = datetime.datetime.now()

            if search_language in ['en', 'no', 'es', 'sv', 'da']:
                language_settings = 'english'
            elif search_language in ['hr', 'sk', 'sl', 'so', 'hu']:
                language_settings = 'czech'
            else:
                language_settings = 'czech'

            sql_outside_query = sql.SQL(
                "SELECT d.url, d.url_decoded, d.title, d.depth, d.pagerank, d.url_length, d.language, ts_headline({language_settings}, d.content, plainto_tsquery({language_settings}, {query}), {headline_settings}) FROM ({inside_query}) AS ids INNER JOIN {index_table} d ON d.hash = ids.hash;")

            sql_inside_query = sql.SQL(
                "SELECT hash, pagerank FROM {index_table}, plainto_tsquery({language_settings}, {query}) AS q WHERE search_index @@ q ORDER BY LOG(ts_rank_cd(search_index, q, (1 | 16))) * 0.6 + LOG(pagerank) * 0.4 DESC OFFSET {offset} LIMIT {limit}")

            sql_inside_query_filled = sql_inside_query.format(
                index_table=sql.Identifier(TABLE_NAME),
                query=sql.Literal(search),
                offset=sql.Literal(((page - 1) * 10)),
                limit=sql.Literal(((page - 1) * 10) + 10),
                language_settings=sql.Literal(language_settings))

            sql_outside_query_filled = sql_outside_query.format(
                query=sql.Literal(search),
                headline_settings=sql.Literal(
                    'HighlightAll=TRUE, MaxFragments=3, \
                    FragmentDelimiter=\" ... \", MaxWords=10, \
                    MinWords=5, ShortWord=4'),
                inside_query=sql_inside_query_filled,
                index_table=sql.Identifier(TABLE_NAME),
                language_settings=sql.Literal(language_settings))

            if page == 1:
                tasks.process_search_query.delay(search, language_settings)

            psql_cursor.execute(sql_outside_query_filled)

            output = psql_cursor.fetchall()
            page_size = len(output)
            end_time = datetime.datetime.now()

            query_time = (end_time - start_time).total_seconds()

            return render_template('search/search.html',
                                   search=search,
                                   output=output,
                                   page=page,
                                   time=query_time,
                                   page_size=page_size)
