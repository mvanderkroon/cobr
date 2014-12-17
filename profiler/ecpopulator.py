import sys
sys.path.append("../common")
sys.path.append("../api")

from objects import ForeignKey, PrimaryKey, Table, Column, Base
import metaclient
from dbreader import mysqlMiner

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pymssql
import configparser
import requests
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

def backspace(n):
	# print((b'\x08' * n).decode(), end='') # use \x08 char to go back
	print('\r' * n,end='')                 # use '\r' to go back

def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	if len(config.sections()) == 0:
		print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
		quit()

	headers = {
		'Content-Type': 'application/json'
	}

	es = Elasticsearch([config['ec']['ip'] + ':' + config['ec']['port']])

	# response = requests.post('http://localhost:9200/projects/fb', data=json.dumps(params), headers=headers)
	# if options.truncate:
		# response = requests.delete('http://localhost:9200/projects')

	reader = metaclient.reader(config['metadb']['connection_string'])
	miner = mysqlMiner(db_catalog=config['subjectdb']['db_catalog'], db_host=config['subjectdb']['db_host'], db_user=config['subjectdb']['db_user'], db_password=config['subjectdb']['db_password'])

	columns = reader.getColumns(filter=Column.db_schema==options.db_catalog)
	
	for i,column in enumerate(columns):
		actions = []

		s = "database progress: %.2f" % round(i/len(columns)*100,2)
		print(s,end="",flush=True)
		backspace(len(s))
		print('')
		
		distinct_values = miner.getDataForColumn(column=column)
		
		if distinct_values is None or len(distinct_values) == 0:
			continue

		for j,value in enumerate(distinct_values):
			s = "\tcolumnprogress: %.2f" % round(j/len(distinct_values)*100,2)
			print(s,end="",flush=True)
			backspace(len(s))

			action = {
				"_index": "projects",
				"_type": "fb",
				"value": str(value),
				"tablename": column.tablename,
				"columnname": column.columnname,
				"db_catalog": column.db_catalog,
				"db_schema": column.db_schema,
				"datatype": column.datatype
			}
			actions.append(action)
			# response = requests.post('http://localhost:9200/projects/fb', data=json.dumps(params), headers=headers)
	
		helpers.bulk(es, actions)
		# backspace(2)
		# print('\033[1A')
		print((b'\x08' * 2).decode(), end='')

if __name__ == '__main__':
	main()