__version__ = "0.4"
__author__ = "James Mills"
__date__ = "3rd February 2011"

import os
import csv
import sys
import optparse

USAGE = "%prog [options] <file>"
VERSION = "%prog v" + __version__

def parse_options():
    parser = optparse.OptionParser(usage=USAGE, version=VERSION)

    parser.add_option("-t", "--table",
            action="store", type="string",
            default=None, dest="table",
            help="Specify table name (defaults to filename)")

    parser.add_option("-f", "--fields",
            action="store", type="string",
            default=None, dest="fields",
            help="Specify a list of fields (comma-separated)")

    parser.add_option("-i", "--filename",
            action="store", type="string",
            default=None, dest="filename",
            help="Specify an input filename")

    parser.add_option("-s", "--skip",
            action="append", type="int",
            default=[], dest="skip",
            help="Specify records to skip (multiple allowed)")

    opts, args = parser.parse_args()

    return opts, args

def generate_rows(f):
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(f.readline())
    f.seek(0)

    reader = csv.reader(f, dialect)
    lines = []
    for line in reader:
        lines.append(line)
    return lines

def create_table(header, opts):
    table_name = opts.table

    sql = "CREATE TABLE IF NOT EXISTS " + table_name + " ("
    for field in header:
        field = field.replace(" ", "_")
        sql += field + " nvarchar(250),"

    sql = sql[:-1] # remove last character
    sql += ");"

    return sql

def main():
    opts, args = parse_options()

    filename = opts.filename

    if filename == "-":
        if opts.table is None:
            print("ERROR: No table specified and stdin used.")
            return
        fd = sys.stdin
        table = opts.table
    else:
        fd = open(filename, "rU")
        if opts.table is None:
            table = os.path.splitext(filename)[0]
        else:
            table = opts.table

    rows = generate_rows(fd)
    header = rows[0]

    create_table_stmt = create_table(header, opts)

    print(create_table_stmt)
    print()

    if opts.fields:
        fields = ",".join([x.strip() for x in opts.fields.split(",")])
    else:
        fields = ",".join(rows[0])

    for i, row in enumerate(rows[1:]):
        if i in opts.skip:
            continue

        values = ", ".join(["\"%s\"" % x.strip() for x in row])
        print("INSERT INTO %s (%s) VALUES (%s);" % (table, fields.replace(' ', '_'), values))

if __name__ == "__main__":
    main()