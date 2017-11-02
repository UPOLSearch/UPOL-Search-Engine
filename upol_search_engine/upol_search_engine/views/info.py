from flask import Blueprint, render_template

mod = Blueprint('info', __name__, url_prefix='/info')


@mod.route('/')
def home():
    return render_template('info/home.html')


@mod.route('/datamining')
def datamining():
    return render_template('info/datamining.html')
