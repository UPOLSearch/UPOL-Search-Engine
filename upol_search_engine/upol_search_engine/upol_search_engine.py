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

        return '{:02}h {:02}m'.format(seconds // 3600, seconds % 3600 // 60)


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

    return jsonify(stage=stage,
                   stage_delta_time=stage_delta_time,
                   total_delta_time=total_delta_time,
                   next_time_start=next_time_start)
