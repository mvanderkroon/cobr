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
import datetime

from optparse import OptionParser
from multiprocessing import Pool

parser = OptionParser()
parser.add_option("-i", "--host", dest="db_host", help="", metavar="string")
parser.add_option("-u", "--user", dest="db_user", help="", metavar="string")
parser.add_option("-p", "--password", dest="db_password", help="", metavar="string")
parser.add_option("-c", "--catalog", dest="db_catalog", help="", metavar="string")
# parser.add_option("-t", "--truncate", dest="truncate", help="", metavar="boolean", default=False)
(options, args) = parser.parse_args()

config = configparser.ConfigParser()
config.read('config.ini')
if len(config.sections()) == 0:
	print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
	quit()

esIp = config['es']['ip']
esPort = config['es']['port']

db_host = options.db_host
db_user = options.db_user
db_password = options.db_password
db_catalog = options.db_catalog

metadb_connectionstring = config['metadb']['connection_string']

def executeOne(column=None):
	es = Elasticsearch(str(esIp) + ':' + str(esPort))

	actions = []
	miner = MetaMiner(type='pymysql').getInstance(db_catalog=db_catalog, db_host=db_host, db_user=db_user, db_password=db_password)
	values = miner.getDataForColumn(column=column, distinct=True)

	for j,value in enumerate(values):
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
	sts = datetime.datetime.now()

	pool = Pool(processes=4)

	# get columns from the meta database
	columns = metaclient.reader(metadb_connectionstring).getColumns(filter=Column.db_catalog==options.db_catalog)

	# execute on multiple threads
	pool.map(executeOne, columns)
	
	print('DONE.' )
	print('time elapsed: ' + str(datetime.datetime.now() - sts))

if __name__ == '__main__':
	main()