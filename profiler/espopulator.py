import sys
sys.path.append("../common")
sys.path.append("../api")

from objects import ForeignKey, PrimaryKey, Table, Column, Base
import metaclient
from dbreader import Factory

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pymssql
import configparser

import json

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-i", "--host", dest="db_host", help="", metavar="string")
parser.add_option("-u", "--user", dest="db_user", help="", metavar="string")
parser.add_option("-p", "--password", dest="db_password", help="", metavar="string")
parser.add_option("-c", "--catalog", dest="db_catalog", help="", metavar="string")
# parser.add_option("-t", "--truncate", dest="truncate", help="", metavar="boolean", default=False)
(options, args) = parser.parse_args()

# NOTES:
# potentially, we could add functionality to insert a document per word contained in text/varchar
# fields to allow a full text search ability on those fields too

def getDataFn(column=None, verbose=False):
	if column is None:
		return

	tdict = {}
	tdict['image'] = "CAST(CAST([{0}] AS BINARY) AS NVARCHAR(MAX))"
	tdict['text'] = "CAST([{0}] AS NVARCHAR(MAX))"
	tdict['ntext'] = "CAST([{0}] AS NVARCHAR(MAX))"

	selectclause = "[{0}]".format(column.columnname)		
	if column.datatype in tdict:
		selectclause = tdict[column.datatype].format(column.columnname)

	retval = []
	with pymssql.connect(options.db_host, options.db_user, options.db_password, options.db_catalog) as conn:
		with conn.cursor() as cursor:
			query = """
				SELECT DISTINCT
					{0}
				FROM 
					[{1}].[{2}].[{3}]
				""".format(selectclause, column.db_catalog, column.db_schema, column.tablename)
			
			if verbose:
				print(query)
				print('')

			cursor.execute(query)
			rows = cursor.fetchall()
			
			if len(rows) > 0:
				retval = [ d[0] for d in rows ]
	return retval

def execute(columns=[], getDataFn=None, es=None):
	print('## espopulator started')
	print('\n'*4)
	for i,column in enumerate(columns):
		actions = []		
		distinct_values = getDataFn(column=column)
		
		if distinct_values is None or len(distinct_values) == 0:
			continue

		for j,value in enumerate(distinct_values):
			sys.stdout.write("\033[4A")

			totalprogress = "\r\033[Ktotal progress {0}/{1}: {2:.2f}% \n".format(i, len(columns), round(i/len(columns)*100,2))
			sys.stdout.write(totalprogress)
			sys.stdout.flush()
			currenttab = "\r\033[Ktable: {0}.{1}.{2} \n".format(column.db_catalog, column.db_schema, column.tablename)
			sys.stdout.write(currenttab)
			sys.stdout.flush()
			currentcol = "\r\033[Kcolumn: {0} \n".format(column.columnname)
			sys.stdout.write(currentcol)
			sys.stdout.flush()
			valueprogress = "\r\033[Kvalueprogress {0}/{1}: {2:.2f}% \n".format(j, len(distinct_values), round(j/len(distinct_values)*100,2))
			sys.stdout.write(valueprogress)
			sys.stdout.flush()

			actions.append({
				"_index": "projects",
				"_type": "fb",
				"value": str(value),
				"tablename": column.tablename,
				"columnname": column.columnname,
				"db_catalog": column.db_catalog,
				"db_schema": column.db_schema,
				"datatype": column.datatype
			})
		helpers.bulk(es, actions)

def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	if len(config.sections()) == 0:
		print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
		quit()
       
	es = Elasticsearch([config['ec']['ip'] + ':' + config['ec']['port']])

	reader = metaclient.reader(config['metadb']['connection_string'])

	miner = Factory(type='pymssql').getInstance(db_catalog=config['subjectdb']['db_catalog'], db_host=config['subjectdb']['db_host'], db_user=config['subjectdb']['db_user'], db_password=config['subjectdb']['db_password'])

	columns = reader.getColumns(filter=Column.db_catalog==options.db_catalog)
	execute(columns=columns, getDataFn=getDataFn, es=es)
	
	print('DONE.')

if __name__ == '__main__':
	main()