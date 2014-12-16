import sys
sys.path.append("../common")
sys.path.append("../api")

from objects import ForeignKey, PrimaryKey, Table, Column, Base
import metaclient
import dbreader

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
(options, args) = parser.parse_args()

def getDataFn(column=None, verbose=False):
	if column is None:
		return

	retval = []
	with pymssql.connect(options.db_host, options.db_user, options.db_password, options.db_catalog) as conn:
		with conn.cursor() as cursor:
			query = """
				SELECT DISTINCT
					[{0}]
				FROM 
					[{1}].[{2}].[{3}]
				""".format(column.columnname, column.db_catalog, column.db_schema, column.tablename)
			
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
	headers = {
		'Content-Type': 'application/json'
	}

	# response = requests.post('http://localhost:9200/projects/fb', data=json.dumps(params), headers=headers)
	# response = requests.delete('http://localhost:9200/projects')

	config = configparser.ConfigParser()
	config.read('config.ini')
	if len(config.sections()) == 0:
		print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
		quit()

	reader = metaclient.reader(config['metadb']['connection_string'])

	columns = reader.getColumns(filter=Column.db_catalog==options.db_catalog)
	es = Elasticsearch()

	print(len(columns))
	for i,column in enumerate(columns):
		actions = []

		s = "database progress: %.2f" % round(i/len(columns)*100,2)
		print(s,end="",flush=True)
		backspace(len(s))
		print('')
		
		distinct_values = getDataFn(column=column)
		
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