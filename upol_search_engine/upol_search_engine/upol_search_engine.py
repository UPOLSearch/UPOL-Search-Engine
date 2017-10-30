from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template
from upol_search_engine.celery_app import next_start_each_n_days
from upol_search_engine.db import mongodb

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/crawler')
def stats():
    return render_template('stats.html')


@app.route('/api/stats')
def api_stats():

    def return_time_or_none(field):
        if field is None:
            return None
        else:
            return field.replace(tzinfo=None)

    def timedelta_to_string(timedelta):
        seconds = timedelta.total_seconds()

        return '{:.0f}h {:.0f}m'.format(seconds // 3600, seconds % 3600 // 60)

    def get_number_or_zero(number):
        if number is None:
            return 0
        else:
            return number

    def get_number_or_na(number):
        if number is None:
            return "N/A"
        else:
            return number

    def thousands_separator(number):
        return '{:,}'.format(number).replace(',', ' ')

    mongodb_client = mongodb.create_client()

    stages = {'finished': 'Naplánováno',
              'killed': 'Selhalo',
              'loading': 'Načítání',
              'crawler': 'Skenování',
              'indexer': 'Indexování',
              'pagerank': 'Pagerank'}

    time = datetime.now()

    stats = mongodb.get_latest_stats(mongodb_client)

    if stats is None:
        target_domain = "N/A"
        next_time_start = "N/A"
        stage_delta_time = "N/A"
        total_delta_time = "N/A"
        stage = stages.get('finished')
        crawler_queue_labels = ['Mimo frontu', 'Ve frontě', 'Navštíveno']
        crawler_queue_values = [0, 0, 0]
        crawler_progress_labels = ['Stránka', 'Alias', 'Soubor', 'Nevalidní', 'Časový limit']
        crawler_progress_values = [0, 0, 0, 0, 0]
        pagerank_graph_deltatime = "N/A"
        pagerank_calculation_deltatime = "N/A"
        pagerank_uploading_deltatime = "N/A"
        indexer_progress = "N/A"
        number_of_domains = "N/A"
        number_of_servers = "N/A"
        number_of_urls = "N/A"
    else:
        target_domain = stats.get('limit_domain')
        result_db = stats.get('progress').get('result')
        stage_db = stats.get('progress').get('stage')

        if result_db == 'running':
            stage = stages.get(stage_db)
        else:
            stage = stages.get(result_db)

        start_time_db = return_time_or_none(stats.get('progress').get('start'))
        end_time_db = return_time_or_none(stats.get('progress').get('end'))

        crawler_start_time_db = return_time_or_none(stats.get('crawler').get('start'))
        crawler_end_time_db = return_time_or_none(stats.get('crawler').get('end'))

        pagerank_start_time_db = return_time_or_none(stats.get('pagerank').get('start'))
        pagerank_end_time_db = return_time_or_none(stats.get('pagerank').get('end'))

        indexer_start_time_db = return_time_or_none(stats.get('indexer').get('start'))
        indexer_end_time_db = return_time_or_none(stats.get('indexer').get('end'))

        run_every_n_days = next_start_each_n_days()
        time_of_next_start = start_time_db + timedelta(days=run_every_n_days)
        next_time_start = timedelta_to_string(time_of_next_start - time)

        if start_time_db is None:
            stage_delta_time = "N/A"
        else:
            if crawler_start_time_db is None:
                stage_delta_time = timedelta_to_string(time - start_time_db)
            else:
                if crawler_end_time_db is None:
                    stage_delta_time = timedelta_to_string(time - crawler_start_time_db)
                else:
                    if pagerank_end_time_db is None:
                        stage_delta_time = timedelta_to_string(time - pagerank_start_time_db)
                    else:
                        if indexer_end_time_db is None:
                            stage_delta_time = timedelta_to_string(time - indexer_start_time_db)

        if end_time_db is None:
            total_delta_time = time - start_time_db
        else:
            total_delta_time = end_time_db - start_time_db
            stage_delta_time = "N/A"

        total_delta_time = timedelta_to_string(total_delta_time)

        if crawler_start_time_db is None:
            crawler_queue_labels = ['Mimo frontu', 'Ve frontě', 'Navštíveno']
            crawler_queue_values = [0, 0, 0]
            crawler_progress_labels = ['Stránka', 'Alias', 'Soubor', 'Nevalidní', 'Časový limit']
            crawler_progress_values = [0, 0, 0, 0, 0]
            number_of_domains = "N/A"
            number_of_servers = "N/A"
            number_of_urls = "N/A"
        else:
            crawler_progress_db = stats.get('crawler').get('progress')

            crawler_queue_labels = ['Mimo frontu', 'Ve frontě', 'Navštíveno']

            visited = get_number_or_zero(crawler_progress_db.get('urls_visited'))
            queued = get_number_or_zero(crawler_progress_db.get('urls_queued'))
            not_queued = get_number_or_zero(crawler_progress_db.get('urls_not_queued'))

            crawler_queue_values = [not_queued, queued, visited]

            crawler_progress_labels = ['Stránka', 'Alias', 'Soubor', 'Nevalidní', 'Časový limit']

            timeout = get_number_or_zero(crawler_progress_db.get('timeout_count'))
            invalid = get_number_or_zero(crawler_progress_db.get('invalid_count'))
            files = get_number_or_zero(crawler_progress_db.get('files_count'))
            aliases = get_number_or_zero(crawler_progress_db.get('aliases_count'))
            pages = visited - timeout - invalid - files - aliases
            number_of_domains = get_number_or_zero(crawler_progress_db.get('number_of_domains'))
            number_of_servers = get_number_or_zero(crawler_progress_db.get('number_of_servers'))
            number_of_urls = thousands_separator(get_number_or_zero(crawler_progress_db.get('urls_count')))

            crawler_progress_values = [pages, aliases, files, invalid, timeout]

        pagerank_progress_db = stats.get('pagerank').get('progress')

        if pagerank_progress_db is None:
            pagerank_graph_deltatime = "N/A"
            pagerank_calculation_deltatime = "N/A"
            pagerank_uploading_deltatime = "N/A"
        else:
            pagerank_graph_starttime = return_time_or_none(pagerank_progress_db.get('building_graph'))
            pagerank_calculation_starttime = return_time_or_none(pagerank_progress_db.get('calculation'))
            pagerank_uploading_starttime = return_time_or_none(pagerank_progress_db.get('uploading'))

            if pagerank_calculation_starttime is not None:
                pagerank_graph_deltatime = timedelta_to_string(pagerank_calculation_starttime - pagerank_start_time_db)
            else:
                pagerank_graph_deltatime = timedelta_to_string(time - pagerank_start_time_db)

            if pagerank_uploading_starttime is not None:
                pagerank_calculation_deltatime = timedelta_to_string(pagerank_uploading_starttime - pagerank_calculation_starttime)
            else:
                if pagerank_calculation_starttime is None:
                    pagerank_calculation_deltatime = "N/A"
                else:
                    pagerank_calculation_deltatime = timedelta_to_string(time - pagerank_calculation_starttime)

            if pagerank_end_time_db is not None:
                pagerank_uploading_deltatime = timedelta_to_string(pagerank_end_time_db - pagerank_uploading_starttime)
            else:
                if pagerank_uploading_starttime is None:
                    pagerank_uploading_deltatime = "N/A"
                else:
                    pagerank_uploading_deltatime = timedelta_to_string(time - pagerank_uploading_starttime)

        indexer_progress_db = stats.get('indexer').get('progress')

        if indexer_progress_db is None:
            indexer_progress = 0
        else:
            indexer_progress = thousands_separator(get_number_or_zero(indexer_progress_db.get('progress')))

    return jsonify(target_domain=target_domain,
                   stage=stage,
                   stage_delta_time=stage_delta_time,
                   total_delta_time=total_delta_time,
                   next_time_start=next_time_start,
                   crawler_progress_labels=crawler_progress_labels,
                   crawler_progress_values=crawler_progress_values,
                   crawler_queue_labels=crawler_queue_labels,
                   crawler_queue_values=crawler_queue_values,
                   indexer_progress=indexer_progress,
                   pagerank_graph_deltatime=pagerank_graph_deltatime,
                   pagerank_calculation_deltatime=pagerank_calculation_deltatime,
                   pagerank_uploading_deltatime=pagerank_uploading_deltatime,
                   number_of_domains=number_of_domains,
                   number_of_servers=number_of_servers,
                   number_of_urls=number_of_urls)
