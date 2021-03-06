import sys, argparse
sys.path.append("../common")

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging

import flask
import flask.ext.sqlalchemy
import flask.ext.restless

from objects import ForeignKey, PrimaryKey, Table, Column

# Create the Flask application and the Flask-SQLAlchemy object.
app = flask.Flask(__name__)

db = flask.ext.sqlalchemy.SQLAlchemy(app)

def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
    return response

# Create the Flask-Restless API manager.
manager = flask.ext.restless.APIManager(app, flask_sqlalchemy_db=db)

# Create API endpoints, which will be available at /api/<tablename> by
# default. Allowed HTTP methods can be specified as well.
manager.create_api(Table, methods=['GET'], results_per_page=-1, allow_functions=True)
manager.create_api(Column, methods=['GET'], results_per_page=-1, allow_functions=True)
manager.create_api(ForeignKey, methods=['GET'], results_per_page=-1, allow_functions=True)
manager.create_api(PrimaryKey, methods=['GET'], results_per_page=-1, allow_functions=True)

app.after_request(add_cors_headers)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="force connection_string for the subject-database, defaults to connection_string in config.ini", metavar="string", default=None)
    parser.add_argument("-p", "--port", help="port for the API to be exposed on, defaults to 5000", metavar="int", default=5000)
    args = parser.parse_args()

    app.config['DEBUG'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = args.src

    enable_pretty_logging()

    server = HTTPServer(WSGIContainer(app))
    server.bind(args.port)
    server.start(0)  # Forks multiple sub-processes
    IOLoop.instance().start()