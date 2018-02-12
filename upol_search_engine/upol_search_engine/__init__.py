from flask import Flask, g, render_template
from upol_search_engine.db import postgresql
from upol_search_engine.upol_search_engine.views import search
from upol_search_engine.upol_search_engine.views import info
from upol_search_engine.upol_search_engine.views import api
from upol_search_engine import settings

app = Flask(__name__)

app.register_blueprint(search.mod)
app.register_blueprint(info.mod)
app.register_blueprint(api.mod)
app.config['ANALYTICS_ID'] = settings.CONFIG.get('General', 'analytics_id')


def get_db():
    """
    Opens a new database connection
    if there is none yet for the current application context.
    """
    if not hasattr(g, 'postgresql_db'):
        g.postgresql_db = postgresql.create_client()
    return g.postgresql_db


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'postgresql_db'):
        g.postgresql_db.cursor().close()
        g.postgresql_db.close()


@app.errorhandler(404)
def page_not_found(e):
    return render_template('error/404.html'), 404


@app.errorhandler(500)
def server_error(e):
    return render_template('error/500.html'), 500
