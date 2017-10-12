from datetime import datetime

from flask import Flask, jsonify, render_template
from upol_search_engine.db import mongodb

app = Flask(__name__)


@app.route('/')
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

    mongodb_client = mongodb.create_client()

    stages = {'finished': 'Scheduled',
              'killed': 'Failed',
              'loading': 'Loading',
              'crawler': 'Crawling',
              'indexer': 'Indexing',
              'pagerank': 'Pagerank'}

    time = datetime.now()

    stats = mongodb.get_latest_stats(mongodb_client)

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

    next_time_start = "N/A"

    if indexer_start_time_db is None:
        if pagerank_end_time_db is None:
            if crawler_start_time_db is None:
                stage_delta_time = "N/A"
            else:
                stage_delta_time = timedelta_to_string(time - crawler_start_time_db)
        else:
            stage_delta_time = timedelta_to_string(time - pagerank_start_time_db)
    else:
        stage_delta_time = timedelta_to_string(time - indexer_start_time_db)

    if end_time_db is None:
        total_delta_time = time - start_time_db
    else:
        total_delta_time = end_time_db - start_time_db
        stage_delta_time = "N/A"

    total_delta_time = timedelta_to_string(total_delta_time)
    # total_delta_time = str(total_delta_time)

    crawler_progress_db = stats.get('crawler').get('progress')

    crawler_progress_labels = ['Pages', 'Aliases','Files', 'Invalid', 'Timeout']

    timeout = get_number_or_zero(crawler_progress_db.get('timeout_count'))
    invalid = get_number_or_zero(crawler_progress_db.get('invalid_count'))
    files = get_number_or_zero(crawler_progress_db.get('files_count'))
    aliases = get_number_or_zero(crawler_progress_db.get('aliases_count'))
    pages = get_number_or_zero(crawler_progress_db.get('urls_count')) - timeout - invalid - files - aliases

    crawler_progress_values = [pages, aliases, files, invalid, timeout]

    crawler_queue_labels = ['Visited', 'Queued', 'Not Queued']

    visited = get_number_or_zero(crawler_progress_db.get('urls_visited'))
    queued = get_number_or_zero(crawler_progress_db.get('urls_queued'))
    not_queued = get_number_or_zero(crawler_progress_db.get('urls_not_queued'))

    crawler_queue_values = [visited, queued, not_queued]

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

        if pagerank_uploading_starttime is not None:
            pagerank_calculation_deltatime = timedelta_to_string(pagerank_uploading_starttime - pagerank_calculation_starttime)

        if pagerank_end_time_db is not None:
            pagerank_uploading_deltatime = timedelta_to_string(pagerank_end_time_db - pagerank_uploading_starttime)

    indexer_progress_db = stats.get('indexer').get('progress')

    if indexer_progress_db is None:
        indexer_progress = 0
        indexer_total = "N/A"
    else:
        indexer_progress = get_number_or_zero(indexer_progress_db.get('progress'))
        indexer_total = get_number_or_na(indexer_progress_db.get('progress'))

    return jsonify(stage=stage,
                   stage_delta_time=stage_delta_time,
                   total_delta_time=total_delta_time,
                   next_time_start=next_time_start,
                   crawler_progress_labels=crawler_progress_labels,
                   crawler_progress_values=crawler_progress_values,
                   crawler_queue_labels=crawler_queue_labels,
                   crawler_queue_values=crawler_queue_values,
                   indexer_progress=indexer_progress,
                   indexer_total=indexer_total,
                   pagerank_graph_deltatime=pagerank_graph_deltatime,
                   pagerank_calculation_deltatime=pagerank_calculation_deltatime,
                   pagerank_uploading_deltatime=pagerank_uploading_deltatime)
