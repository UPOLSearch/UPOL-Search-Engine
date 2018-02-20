import datetime
import re

from flask import Blueprint, render_template, request
from langdetect import detect, lang_detect_exception
from psycopg2 import sql
from upol_search_engine import settings, upol_search_engine

mod = Blueprint('search', __name__, url_prefix='/')


@mod.route('/',  methods=['GET', 'POST'])
def home():
    TABLE_NAME = settings.CONFIG.get('General', 'postgresql_table_name')

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

            try:
                search_language = detect(search)
            except lang_detect_exception.LangDetectException as e:
                # Fallback language
                search_language = 'cs'

            if page is None:
                page = 1
            else:
                page = int(page)

            start_time = datetime.datetime.now()

            if search_language in ['en', 'no', 'es', 'sv', 'da']:
                language_settings = 'english'
            elif search_language in ['cs', 'hr', 'sk', 'sl', 'so', 'hu']:
                language_settings = 'czech'
            else:
                language_settings = 'czech'

            sql_outside_query = sql.SQL(
                "SELECT d.url, d.url_decoded, d.title, d.depth, d.pagerank, d.url_length, d.language, d.file_type, ts_headline({language_settings}, d.content, plainto_tsquery({language_settings}, {query}), {headline_settings}) FROM ({inside_query}) AS ids INNER JOIN {index_table} d ON d.hash = ids.hash;")

            sql_inside_query = sql.SQL(
                "SELECT hash, pagerank FROM {index_table}, plainto_tsquery({language_settings}, {query}) AS q WHERE search_index @@ q ORDER BY LOG(ts_rank_cd(search_index, q, (1 | 4))) * 0.6 + LOG(pagerank) * 0.4 DESC OFFSET {offset} LIMIT {limit}")

            sql_inside_query_filled = sql_inside_query.format(
                index_table=sql.Identifier(TABLE_NAME),
                query=sql.Literal(search),
                offset=sql.Literal(((page - 1) * 10)),
                limit=sql.Literal(10),
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

            psql_cursor.execute(sql_outside_query_filled)

            output = psql_cursor.fetchall()

            sql_metadata_query = sql.SQL("SELECT type, json FROM metadata WHERE microformat_index @@ to_tsquery('czech', {query}) ORDER BY ts_rank(microformat_index, to_tsquery('czech', {query})) DESC LIMIT 3;")

            search_metadata = to_tsquery_with_or(search)

            sql_metadata_query_filled = sql_metadata_query.format(
                query=sql.Literal(search_metadata)
            )

            psql_cursor.execute(sql_metadata_query_filled)

            output_metadata = psql_cursor.fetchall()

            formated_metadata = []

            for metadata in output_metadata:
                formated_metadata.append(format_metadata(metadata))

            page_size = len(output)
            end_time = datetime.datetime.now()

            query_time = (end_time - start_time).total_seconds()

            return render_template('search/search.html',
                                   search=search,
                                   output=output,
                                   metadata=formated_metadata,
                                   page=page,
                                   time=query_time,
                                   page_size=page_size)


def to_tsquery_with_or(search_query):
    splitted = re.split('\s+', search_query)
    splitted = list(filter(None, splitted))
    final_query = ""

    for i in range(len(splitted)):
        if i != len(splitted) - 1:
            final_query += splitted[i] + ' | '
        else:
            final_query += splitted[i]

    return final_query


def format_metadata(metadata):
    metadata_types = {
        'employee': 'Zaměstnanec',
        'department': 'Katedra',
        'class': 'Předmět'}
    metadata_keys = {
        'name': 'Jméno',
        'url': 'Web',
        'phone': 'Telefon',
        'email': 'E-mail',
        'office': 'Kancelář',
        'abbreviation': 'Zkratka předmětu'
    }
    metadata_order = {
        'employee': ['url', 'email', 'phone', 'office'],
        'department': ['url'],
        'class': ['url', 'abbreviation']
    }

    result = []

    metadata_type = metadata[0].lower()
    metadata = metadata[1]

    result.append(metadata.get('name'))
    result.append(metadata_types.get(metadata_type))

    rest_of_metadata = []

    for key in metadata_order.get(metadata_type):
        if metadata.get(key) == "" or metadata.get(key) is None:
            continue

        if key == 'url':
            data = '<a href="{}">{}</a>'.format(metadata.get(key), metadata.get(key))
        elif key == 'email':
            data = '<a href="mailto:{}">{}</a>'.format(metadata.get(key), metadata.get(key))
        elif key == 'phone':
            data = '<a href="tel:{}">{}</a>'.format(metadata.get(key), metadata.get(key))
        else:
            data = metadata.get(key)

        rest_of_metadata.append([metadata_keys.get(key), data])

    result.append(rest_of_metadata)

    return result
