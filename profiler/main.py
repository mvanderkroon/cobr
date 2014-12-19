import sys
sys.path.append("../common")
sys.path.append("../api")
sys.path.append("Processors")
sys.path.append("Miners")

from MetaMiner import MetaMiner

from ColumnProcessor import ColumnProcessor
from NumpyColumnProcessor import NumpyColumnProcessor

from PostProcessor import PostProcessor
from SimplePostProcessor import SimplePostProcessor

from TableProcessor import TableProcessor

import metaclient

import datetime
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
if len(config.sections()) == 0:
	print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
	quit()

def main():	
	sts = datetime.datetime.now()

	miner = MetaMiner(type='pymysql').getInstance(db_catalog=config['subjectdb']['db_catalog'], db_host=config['subjectdb']['db_host'], db_user=config['subjectdb']['db_user'], db_password=config['subjectdb']['db_password'])
	writer = metaclient.writer(config['metadb']['connection_string'])

	columns = miner.getColumns()
	tables = miner.getTables()
	fks = miner.getForeignKeys()
	pks = miner.getPrimaryKeys()

	print('cols: ' + str(len(columns)))
	print('tabs: ' + str(len(tables)))
	print('fks: ' + str(len(fks)))
	print('pks: ' + str(len(pks)))

	# TBD: load all columns/tables at this point, then incrementally merge() as below operations become done

	ColumnProcessor(columns=columns, getDataFn=miner.getDataForColumn, processor=NumpyColumnProcessor).execute()
	PostProcessor(tables=tables, columns=columns, explicit_primarykeys=pks, explicit_foreignkeys=fks, processor=SimplePostProcessor)
	TableProcessor(tables=tables, processor=miner).execute()

	# writer.reset() # dropping the database tables and recreating them

	# uncomment this for actual writing to database
	# writer.writeTables(tables)
	# writer.writeColumns(columns)
	# writer.writePrimaryKeys(pks)
	# writer.writeForeignKeys(fks)
	# writer.close()

	print('time elapsed: ' + str(datetime.datetime.now() - sts))

if __name__ == '__main__':
	main()
