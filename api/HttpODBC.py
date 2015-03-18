from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging

import sys, csv, io, argparse, json
sys.path.append("../common")

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from flask import Flask, after_this_request, request, Response
from flask_cors import CORS

from csvkit import sql, table, CSVKitWriter

app = Flask(__name__)
cors = CORS(app)

@app.route('/primarykey', methods=['GET'])
def listprimarykeys():
    insp = reflection.Inspector.from_engine(engine)
    return json.dumps([insp.get_primary_keys(tablename) for tablename in insp.get_table_names() if len(insp.get_primary_keys(tablename)) > 0])

@app.route('/foreignkey', methods=['GET'])
def listforeignkeys():
    insp = reflection.Inspector.from_engine(engine)
    return json.dumps([insp.get_foreign_keys(tablename) for tablename in insp.get_table_names() if len(insp.get_foreign_keys(tablename)) > 0])

@app.route('/view', methods=['GET'])
def listviews():
    insp = reflection.Inspector.from_engine(engine)
    return json.dumps(insp.get_view_names())

@app.route('/view/<viewname>', methods=['GET'])
def viewdata(viewname):
    insp = reflection.Inspector.from_engine(engine)
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

@app.route('/table', methods=['GET'])
def listtables():
    insp = reflection.Inspector.from_engine(engine)
    return json.dumps(insp.get_table_names())

@app.route('/table/<tablename>', methods=['GET', 'POST'])
def tabledata(tablename):
    insp = reflection.Inspector.from_engine(engine)

    def allowed_file(filename):
        return '.' in filename and \
               filename.rsplit('.', 1)[1] in ALLOWED_UPLOAD_EXTENSIONS

    if request.method == 'POST':
        # fire requests like this:
        # curl -v -i -X POST -H "Content-Type: multipart/form-data" -F "file=@/Users/matthijs/Desktop/data.csv" 127.0.0.1:5001/table/testdata

        encoding = 'utf-8'
        if request.args.get('encoding') is not None:
            encoding = request.args.get('encoding')

        file = request.files['file']
        if file and allowed_file(file.filename):
            return csv2sql(file=file, db_schema=None, tablename=tablename, encoding=encoding)

        return "You either didn't POST a file, or the filetype isn't allowed"

    elif request.method == 'GET':
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

def csv2sql(file=None, db_schema=None, tablename=None, encoding='utf-8', snifflimit=512*1024):

    try:
        conn = engine.connect()
        trans = conn.begin()

        csv_table = table.Table.from_csv(
            file.stream,
            name=tablename,
            snifflimit=snifflimit,
            blanks_as_nulls=True,
            infer_types=True,
            no_header_row=False,
            encoding=encoding
        )

        sql_table = sql.make_table(
            csv_table,
            tablename,
            False, #self.args.no_constraints
            db_schema, #self.args.db_schema
            metadata
        )

        sql_table.create()

        insert = sql_table.insert()
        headers = csv_table.headers()

        conn.execute(insert, [dict(zip(headers, row)) for row in csv_table.to_rows()])
        trans.commit()

    except Exception as e:
        trans.rollback()
        print(e)
        return e
    finally:
        conn.close()
        file.close()
        return json.dumps(tablename)

def sql2csv(name=None, delimiter=',', quotechar='"'):
    insp = reflection.Inspector.from_engine(engine)

    output = io.StringIO()
    writer = csv.writer(output, delimiter=str(delimiter), quotechar=str(quotechar), quoting=csv.QUOTE_ALL)

    connection = engine.connect()
    res = connection.execute("SELECT * FROM " + name)

    # write header
    writer.writerow([c['name'] for c in insp.get_columns(name)])

    c = 0
    while True:
        row = res.fetchone()
        if row is None:
            break
        writer.writerow(row)

    retval = output.getvalue()

    connection.close()
    output.close()

    return retval

ALLOWED_UPLOAD_EXTENSIONS = set(['csv'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="force connection_string for the subject-database, defaults to connection_string in config.ini", metavar="string", default=None)
    parser.add_argument("-p", "--port", help="port for the API to be exposed on, defaults to 5001", metavar="int", default=5001)
    args = parser.parse_args()

    engine, metadata = sql.get_connection(args.src)

    enable_pretty_logging()
    server = HTTPServer(WSGIContainer(app), max_buffer_size=4000*1024*1024, max_body_size=4000*1024*1024)
    server.bind(args.port)
    server.start(0)  # Forks multiple sub-processes
    IOLoop.instance().start()