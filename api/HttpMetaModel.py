import sys, argparse, ConfigParser
sys.path.append("../common")

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

import flask
import flask.ext.sqlalchemy
import flask.ext.restless
from flask.ext.compress import Compress

from objects import ForeignKey, PrimaryKey, Table, Column

# Create the Flask application and the Flask-SQLAlchemy object.
app = flask.Flask(__name__)
Compress(app)

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
manager.create_api(Table, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)
manager.create_api(Column, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)
manager.create_api(ForeignKey, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)
manager.create_api(PrimaryKey, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)

app.after_request(add_cors_headers)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="force connection_string for the subject-database, defaults to connection_string in config.ini", metavar="string", default=None)
    parser.add_argument("-p", "--port", help="port for the API to be exposed on, defaults to 5000", metavar="int", default=5000)
    args = parser.parse_args()

    connection_string = args.src
    if connection_string is None:
        config = ConfigParser.ConfigParser()
        config.read('config.ini')
        if len(config.sections()) == 0:
            print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
            quit()

        connection_string = config.get('RESTAPI', 'connection_string')

    app.config['DEBUG'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = connection_string

    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(args.port)
    IOLoop.instance().start()