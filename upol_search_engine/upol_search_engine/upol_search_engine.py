from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, send_from_directory

app = Flask(__name__)
#
#
# @app.route('/images/<path:path>')
# def send_images(path):
#     return send_from_directory('static/images', path)
