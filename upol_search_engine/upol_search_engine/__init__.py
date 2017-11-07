from flask import Flask, g
from upol_search_engine.db import postgresql

app = Flask(__name__)

from upol_search_engine.upol_search_engine.views import search
from upol_search_engine.upol_search_engine.views import info
from upol_search_engine.upol_search_engine.views import api

app.register_blueprint(search.mod)
app.register_blueprint(info.mod)
app.register_blueprint(api.mod)

def get_db():
    """Opens a new database connection if there is none yet for the current application context.
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
