import sys
sys.path.append("../common")

import csv
import io

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from flask import Flask
from flask import request
from flask import Response

from flask_cors import CORS
from flask.ext.compress import Compress

import ConfigParser
import unicodecsv

config = ConfigParser.ConfigParser()
config.read('config.ini')
if len(config.sections()) == 0:
    print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
    quit()

engine = create_engine(config.get('DATAAPI', 'connection_string'), pool_size=3, pool_recycle=3600)
insp = reflection.Inspector.from_engine(engine)

app = Flask(__name__)
cors = CORS(app)
Compress(app)

def sql2csv(name, delimiter, quotechar):
    connection = engine.connect()
    rows = connection.execute("SELECT * FROM " + name).fetchall()
    connection.close()

    output = io.BytesIO()
    writer = unicodecsv.writer(output, delimiter=str(delimiter), quotechar=str(quotechar), quoting=csv.QUOTE_ALL, encoding='utf-8')

    # write header
    writer.writerow([c['name'] for c in insp.get_columns(name)])

    for row in rows:
        writer.writerow(row)
    return output.getvalue()

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

if __name__ == "__main__":
    app.run()
