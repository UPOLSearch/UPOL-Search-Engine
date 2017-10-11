from datetime import datetime

from flask import Flask, jsonify, render_template
from upol_search_engine.db import mongodb

app = Flask(__name__)


@app.route('/')
def stats():
    return render_template('stats.html')


@app.route('/api/stats')
def api_stats():
    mongodb_client = mongodb.create_client()

    stages = {'finished': 'Scheduled',
              'killed': 'Failed',
              'loading': 'Loading',
              'crawler': 'Crawling',
              'indexer': 'Indexing',
              'pagerank': 'Pagerank'}

    time = datetime.now()

    stats = mongodb.get_lastest_stats(mongodb_client)

    result_db = stats.get('progress').get('result')
    stage_db =  stats.get('progress').get('stage')

    if result_db == 'running':
        stage = stages.get(stage_db)
    else:
        stage = stages.get(result_db)

    start_time_db = stats.get('progress').get('start')
    end_time_db = stats.get('progress').get('start')

    crawler_start_time_db = stats.get('crawler').get('start')
    crawler_end_time_db = stats.get('crawler').get('start')

    pagerank_start_time_db = stats.get('pagerank').get('start')
    pagerank_end_time_db = stats.get('pagerank').get('start')

    indexer_start_time_db = stats.get('indexer').get('start')
    indexer_end_time_db = stats.get('indexer').get('start')

    total_delta_time =
