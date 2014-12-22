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
parser.add_option("-d", "--db_dialect", dest="db_dialect", help="'pymssql' or 'pymysql'", metavar="string")
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
db_dialect = options.db_dialect

metadb_connectionstring = config['metadb']['connection_string']

excluded = ['image']

def executeOne(column=None):
	if column.datatype not in excluded:
		es = Elasticsearch(host=str(esIp), port=str(esPort), timeout=120)

		actions = []
		miner = MetaMiner(type=db_dialect).getInstance(db_catalog=db_catalog, db_host=db_host, db_user=db_user, db_password=db_password)
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

	pool = Pool(processes=64)

	# get columns from the meta database
	columns = metaclient.reader(metadb_connectionstring).getColumns(filter=Column.db_catalog==options.db_catalog)
	print('## going to populate Elastic Search with {0} columns'.format(len(columns)-1))
	print('\n'*1)

	# execute on multiple threads
	for i, _ in enumerate(pool.imap_unordered(executeOne, columns)):
		sys.stdout.write("\033[1A")

		totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(columns)-1, round(i/(len(columns)-1)*100,2))
		sys.stdout.write(totalprogress)
		sys.stdout.flush()
	
	print('')	
	print('## DONE.' )
	print('## time elapsed: {0}'.format(str(datetime.datetime.now() - sts)))

if __name__ == '__main__':
	main()