import sys
sys.path.append("../common")
sys.path.append("../api")
sys.path.append("Miners")

from objects import ForeignKey, PrimaryKey, Table, Column, Base
import metaclient
from MetaMiner import MetaMiner

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

def execute(columns=[], getDataFn=None, es=None):
	print('## espopulator started')
	print('\n'*4)
	for i,column in enumerate(columns):
		actions = []		
		distinct_values = getDataFn(column=column, distinct=True)
		
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
       
	es = Elasticsearch([config['es']['ip'] + ':' + config['es']['port']])

	reader = metaclient.reader(config['metadb']['connection_string'])
	miner = MetaMiner(type='pymysql').getInstance(db_catalog=config['subjectdb']['db_catalog'], db_host=config['subjectdb']['db_host'], db_user=config['subjectdb']['db_user'], db_password=config['subjectdb']['db_password'])

	columns = reader.getColumns(filter=Column.db_catalog==options.db_catalog)
	execute(columns=columns, getDataFn=miner.getDataForColumn, es=es)
	
	print('DONE.')

if __name__ == '__main__':
	main()