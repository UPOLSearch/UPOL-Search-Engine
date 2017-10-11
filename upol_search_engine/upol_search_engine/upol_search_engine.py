from flask import Flask, jsonify, render_template
from upol_search_engine.db import mongodb

app = Flask(__name__)


@app.route('/')
def stats():
    return render_template('stats.html')


# @app.route('/api/stats')
# def api_stats():
#     mongodb_client = mongodb.create_client()
