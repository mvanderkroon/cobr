import sys
sys.path.append("../common")

import csv
import io

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from flask import Flask
from flask import request

from flask_cors import CORS

import ConfigParser

config = ConfigParser.ConfigParser()
config.read('config.ini')
if len(config.sections()) == 0:
    print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
    quit()

engine = create_engine(config.get('DATAAPI', 'connection_string'))
insp = reflection.Inspector.from_engine(engine)

app = Flask(__name__)
cors = CORS(app)

def sql2csv(name, delimiter, quotechar):
    connection = engine.connect()
    query = connection.execute("SELECT * FROM " + name).fetchall()
    connection.close()

    output = io.BytesIO()
    writer = csv.writer(output, delimiter=str(delimiter), quotechar=str(quotechar), quoting=csv.QUOTE_ALL)

    writer.writerow([c['name'] for c in insp.get_columns(name)])

    for row in query:
        writer.writerow(row)
    return output.getvalue()

@app.route('/view')
def sqlviewoverview():
    return str(insp.get_view_names())

@app.route('/view/<viewname>')
def sqlview2csv(viewname):
    if viewname in insp.get_view_names():
        delimiter = ','
        if request.args.get('delimiter') is not None:
            delimiter = request.args.get('delimiter')

        quotechar = '"'
        if request.args.get('quotechar') is not None:
            quotechar = request.args.get('quotechar')

        return sql2csv(viewname, delimiter, quotechar)
    else:
        return "Not a valid viewname"

@app.route('/table')
def sqltableoverview():
    return str(insp.get_table_names())

@app.route('/table/<tablename>')
def sqltable2csv(tablename):
    if tablename in insp.get_table_names():
        delimiter = ','
        if request.args.get('delimiter') is not None:
            delimiter = request.args.get('delimiter')

        quotechar = '"'
        if request.args.get('quotechar') is not None:
            quotechar = request.args.get('quotechar')

        return sql2csv(tablename, delimiter, quotechar)
    else:
        return "Not a valid tablename"

if __name__ == "__main__":
    app.run()