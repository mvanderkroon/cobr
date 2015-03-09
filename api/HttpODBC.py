from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

import sys, csv, io, argparse, ConfigParser, unicodecsv
sys.path.append("../common")

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from flask import Flask
from flask import request
from flask import Response

from flask_cors import CORS
from flask.ext.compress import Compress

app = Flask(__name__)
cors = CORS(app)
Compress(app)

@app.route('/primarykey')
def listprimarykeys():
    pks = []
    for tablename in insp.get_table_names():
        pks.append(insp.get_primary_keys(tablename))
    return str(pks)

@app.route('/foreignkey')
def listforeignkeys():
    fks = []
    for tablename in insp.get_table_names():
        fks.append(insp.get_foreign_keys(tablename))
    return str(fks)

@app.route('/view')
def listviews():
    return str(insp.get_view_names())

@app.route('/view/<viewname>')
def viewdata(viewname):
    if viewname in insp.get_view_names():
        delimiter = ','
        if request.args.get('delimiter') is not None:
            delimiter = request.args.get('delimiter')

        quotechar = '"'
        if request.args.get('quotechar') is not None:
            quotechar = request.args.get('quotechar')

        return Response(sql2csv(viewname, delimiter, quotechar), mimetype='text/csv')
    else:
        return "Not a valid viewname"

@app.route('/table')
def listtables():
    return str(insp.get_table_names())

@app.route('/table/<tablename>')
def tabledata(tablename):
    if tablename in insp.get_table_names():
        delimiter = ','
        if request.args.get('delimiter') is not None:
            delimiter = request.args.get('delimiter')

        quotechar = '"'
        if request.args.get('quotechar') is not None:
            quotechar = request.args.get('quotechar')

        return Response(sql2csv(tablename, delimiter, quotechar), mimetype='text/csv')
    else:
        return "Not a valid tablename"

def sql2csv(name, delimiter, quotechar):
    output = io.BytesIO()
    writer = unicodecsv.writer(output, delimiter=str(delimiter), quotechar=str(quotechar), quoting=csv.QUOTE_ALL, encoding='utf-8')

    connection = engine.connect()
    res = connection.execute("SELECT * FROM " + name)

    # write header
    writer.writerow([c['name'] for c in insp.get_columns(name)])

    while True:
        row = res.fetchone()
        if row is None:
            break
        writer.writerow(row)
    connection.close()

    return output.getvalue()

engine = None
insp = None

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="force connection_string for the subject-database, defaults to connection_string in config.ini", metavar="string", default=None)
    parser.add_argument("-p", "--port", help="port for the API to be exposed on, defaults to 5001", metavar="int", default=5001)
    args = parser.parse_args()

    connection_string = args.src
    if connection_string is None:
        config = ConfigParser.ConfigParser()
        config.read('config.ini')
        if len(config.sections()) == 0:
            print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
            quit()

        connection_string = config.get('DATAAPI', 'connection_string')

    engine = create_engine(connection_string, pool_size=3, pool_recycle=3600)
    insp = reflection.Inspector.from_engine(engine)

    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(args.port)
    IOLoop.instance().start()
