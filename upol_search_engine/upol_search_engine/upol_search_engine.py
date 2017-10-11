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
    end_time_db = return_time_or_none(stats.get('progress').get('start'))

    crawler_start_time_db = return_time_or_none(stats.get('crawler').get('start'))
    crawler_end_time_db = return_time_or_none(stats.get('crawler').get('start'))

    pagerank_start_time_db = return_time_or_none(stats.get('pagerank').get('start'))
    pagerank_end_time_db = return_time_or_none(stats.get('pagerank').get('start'))

    indexer_start_time_db = return_time_or_none(stats.get('indexer').get('start'))
    indexer_end_time_db = return_time_or_none(stats.get('indexer').get('start'))

    if end_time_db is None:
        total_delta_time = time - start_time_db
    else:
        total_delta_time = end_time_db - start_time_db

    next_time_start = "N/A"

    if indexer_start_time_db is None:
        if pagerank_end_time_db is None:
            if crawler_start_time_db is None:
                stage_delta_time = "N/A"
            else:
                stage_delta_time = time - crawler_start_time_db
        else:
            stage_delta_time = time - pagerank_start_time_db
    else:
        stage_delta_time = time - indexer_start_time_db

    return jsonify(stage=stage,
                   stage_delta_time=stage_delta_time,
                   total_delta_time=total_delta_time,
                   next_time_start=next_time_start)
