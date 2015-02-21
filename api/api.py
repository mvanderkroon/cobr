import sys
sys.path.append("../common")

import flask
import flask.ext.sqlalchemy
import flask.ext.restless
# from flask.ext.gzip import Gzip

from objects import ForeignKey, PrimaryKey, Table, Column

import configparser
config = configparser.ConfigParser()
config.read('config.ini')
if len(config.sections()) == 0:
    print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
    quit()

# Create the Flask application and the Flask-SQLAlchemy object.
app = flask.Flask(__name__)

app.config['DEBUG'] = True

app.config['SQLALCHEMY_DATABASE_URI'] = config.get('RESTAPI', 'connection_string')
db = flask.ext.sqlalchemy.SQLAlchemy(app)

def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
    return response

# Create the database tables.
# db.create_all()

# Create the Flask-Restless API manager.
manager = flask.ext.restless.APIManager(app, flask_sqlalchemy_db=db)

# Create API endpoints, which will be available at /api/<tablename> by
# default. Allowed HTTP methods can be specified as well.
manager.create_api(Table, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)
manager.create_api(Column, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)
manager.create_api(ForeignKey, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)
manager.create_api(PrimaryKey, methods=['GET', 'POST', 'PUT', 'DELETE'], results_per_page=-1, allow_functions=True)

app.after_request(add_cors_headers)

# gzip = Gzip(app)

if __name__ == '__main__':
    # start the flask loop
    app.run(host=config.get('RESTAPI', 'ip'), threaded=True)
