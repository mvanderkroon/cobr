import sys
sys.path.append("../common")
sys.path.append("../api")

from dbreader import mssqlMiner
from columnprocessor import NumpyColumnProcessor
from columnprocessor import MssqlColumnProcessor
from tableprocessor import MssqlTableProcessor
from postprocessor import PostProcessor
import metaclient

import datetime
import configparser

import os

config = configparser.ConfigParser()
config.read('config.ini')
if len(config.sections()) == 0:
	print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
	quit()

def main():
	sts = datetime.datetime.now()

	miner = mssqlMiner(db_catalog=config['subjectdb']['db_catalog'], db_host=config['subjectdb']['db_host'], db_user=config['subjectdb']['db_user'], db_password=config['subjectdb']['db_password'])
	writer = metaclient.writer(config['metadb']['connection_string'])

	fks = miner.getForeignKeys()
	pks = miner.getPrimaryKeys()
	tables = miner.getTables()
	columns = miner.getColumns()

	# TBD: load all columns/tables at this point, then incrementally merge() as below operations become done
	print('datamodel mined, proceeding to column processing step ')

	for i,column in enumerate(columns):
		try:
			print('processing {0} of {1:5}: {2}.{3}.{4}.{5}'.format(i+1, len(columns), column.db_catalog, column.db_schema, column.tablename, column.columnname))
			data = miner.getDataForColumn(column)
			columnProcessor = NumpyColumnProcessor(data=data, column=column)
			statdict = columnProcessor.doOperations()

			column.min = statdict['min']
			column.max = statdict['max']
			column.avg = statdict['avg']
			column.stdev = statdict['stdev']
			column.sum = statdict['sum']
			column.mode = statdict['mode']
			column.variance = statdict['var']
			column.median = statdict['median']
			column.num_nulls = statdict['num_nulls']
			column.num_distinct_values = statdict['num_distinct_values']
			column.num_positive = statdict['num_positive']
			column.num_negative = statdict['num_negative']
			column.num_zero = statdict['num_zero']
			column.start_date = statdict['start_date']
			column.end_date = statdict['end_date']
			column.lifespan_in_days = statdict['lifespan_in_days']
		except:
			print('failed to process: {0}.{1}.{2}.{3}'.format(column.db_catalog, column.db_schema, column.tablename, column.columnname))
		
	postProcessor = PostProcessor(tables=tables, columns=columns, explicit_primarykeys=pks, explicit_foreignkeys=fks)

	print('columns processed, proceeding to table processing step ')

	for i,table in enumerate(tables):
		try:
			print('processing {0} of {1:5}: {2}.{3}.{4}'.format(i+1, len(tables), table.db_catalog, table.db_schema, table.tablename))
			tableProcessor = MssqlTableProcessor(db_host=config['subjectdb']['db_host'], db_user=config['subjectdb']['db_user'], db_password=config['subjectdb']['db_password'], table=table)
			table.num_explicit_inlinks = len(postProcessor.getNumExplicitInlinksForTable(table))
			table.num_explicit_outlinks = len(postProcessor.getNumExplicitOutlinksForTable(table))
			table.num_rows = tableProcessor.getTableRowCount()
			table.num_columns = tableProcessor.getColumnCountForTable()
			table.num_emptycolumns = len(postProcessor.getTableNullColumns(table))
			table.tablefillscore = postProcessor.getTableFillScore(table)
		except:
			print('failed to process: {0}.{1}.{2}'.format(table.db_catalog, table.db_schema, table.tablename))

	# writer.reset() # dropping the database tables and recreating them

	print('tables processed, proceeding to write results to database ')

	# uncomment this for actual writing to database
	writer.writeTables(tables)
	writer.writeColumns(columns)
	writer.writePrimaryKeys(pks)
	writer.writeForeignKeys(fks)
	writer.close()

	print('time elapsed: ' + str(datetime.datetime.now() - sts))

if __name__ == '__main__':
	main()
