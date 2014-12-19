import sys
sys.path.append("../../common")
from objects import ForeignKey, PrimaryKey, Table, Column

import pymysql

class MysqlMetaMiner():

	def __init__(self, db_catalog='', db_host='127.0.0.1', db_user='', db_password=''):
		self.db_catalog = db_catalog
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getTableRowCount(self, table=None):
		retval = float('nan')
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=table.db_schema) as cursor:
			cursor.execute(""" 
				SELECT 
					COUNT(*) 
				FROM `{0}`.`{1}`""".format(table.db_catalog, table.tablename))
			retval = cursor.fetchone()[0]
		return retval

	def getColumnCountForTable(self, table=None):
		retval = float('nan')
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=table.db_schema) as cursor:
			cursor.execute(""" 
				SELECT 
					COUNT(*)
				FROM 
					INFORMATION_SCHEMA.COLUMNS 
				WHERE 
					TABLE_SCHEMA = '{0}' 
				AND 
					TABLE_NAME = '{1}'""".format(table.db_catalog, table.tablename))
			retval = cursor.fetchone()[0]
		return retval

	def getDataForTable(self, table=None, verbose=False, distinct=False, order=None):
		if table is None:
			return

		retval = []
		conn = pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=table.db_schema)
		cursor = conn.cursor(pymysql.cursors.DictCursor)
		try:
			query = """
				SELECT 
					*
				FROM 
					`{0}`.`{1}`
				""".format(table.db_catalog, table.tablename)
			
			if verbose:
				print(query)
				print('')

			cursor.execute(query)
			retval = [ d for d in cursor.fetchall() ]
		except Exception as e:
			print(e)
		finally:
			conn.close()
			cursor.close()
			
		return retval

	def getDataForColumn(self, column=None, verbose=False, distinct=False, order=None):
		if column is None:
			return None

		distinctStr = ''
		if distinct:
			distinctStr = 'DISTINCT '

		orderByStr = ''
		if order:
			orderByStr = 'ORDER BY `{0}` {1} '.format(column.columnname, order)
		
		retval = []
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.db_catalog) as cursor:
			query = """
				SELECT {0} 
					`{1}` 
				FROM 
					`{2}`.`{3}` 
				{4} 
				""".format(distinctStr, column.columnname, column.db_catalog, column.tablename, orderByStr)
			
			if verbose:
				print(query)
				print('')

			cursor.execute(query)
			retval = [ d[0] for d in cursor.fetchall() ]

		return retval;

	def getTables(self):
		retval = []
		
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.db_catalog) as cursor:
			cursor.execute("""
				SELECT 
					TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
				FROM
					INFORMATION_SCHEMA.TABLES 
				WHERE
					TABLE_SCHEMA = '{0}' AND TABLE_TYPE = 'BASE TABLE' 
			""".format(self.db_catalog))
			retval = [ Table(db_catalog=d[1], db_schema='', tablename=d[2]) for d in cursor.fetchall() ]
		return retval

	def getColumns(self):
		retval = []
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.db_catalog) as cursor:
			cursor.execute("""
				SELECT 
					T.TABLE_CATALOG, C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE, C.ORDINAL_POSITION
				FROM
					INFORMATION_SCHEMA.COLUMNS C
				JOIN
					INFORMATION_SCHEMA.TABLES T
				ON
					C.TABLE_CATALOG = T.TABLE_CATALOG and C.TABLE_SCHEMA = T.TABLE_SCHEMA and C.TABLE_NAME = T.TABLE_NAME
				WHERE
					T.TABLE_SCHEMA = '{0}' AND T.TABLE_TYPE = 'BASE TABLE' 
				""".format(self.db_catalog))
			retval = [ Column(db_catalog=d[1], db_schema='', tablename=d[2], columnname=d[3], datatype=d[4], ordinal_position=d[5]) for d in cursor.fetchall() ]
		return retval

	def getPrimaryKeys(self, columnseparator='|'):
		retval = []
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.db_catalog) as cursor:
			cursor.execute("""
				SELECT
					TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR '{0}')
				FROM 
					INFORMATION_SCHEMA.KEY_COLUMN_USAGE
				WHERE 
					CONSTRAINT_NAME = 'PRIMARY' and TABLE_SCHEMA = '{1}'
				GROUP BY
					TABLE_NAME, TABLE_SCHEMA
			""".format(columnseparator, self.db_catalog))
			retval = [ PrimaryKey(db_catalog=d[1], db_schema='', tablename=d[2], keyname='expk_' + str(d[2]), db_columns=d[3].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
		return retval

	def getForeignKeys(self, columnseparator='|'):
		retval = []
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.db_catalog) as cursor:
			cursor.execute(""" 
				SELECT
					TABLE_CATALOG, TABLE_SCHEMA, REFERENCED_TABLE_SCHEMA, TABLE_NAME, REFERENCED_TABLE_NAME, CONSTRAINT_NAME, GROUP_CONCAT(COLUMN_NAME SEPARATOR '{0}'), GROUP_CONCAT(REFERENCED_COLUMN_NAME SEPARATOR '{0}')
				FROM 
					INFORMATION_SCHEMA.KEY_COLUMN_USAGE
				WHERE 
					TABLE_SCHEMA = '{1}'
					AND
					REFERENCED_TABLE_NAME IS NOT NULL
					AND
					REFERENCED_TABLE_SCHEMA IS NOT NULL
					AND
					REFERENCED_COLUMN_NAME IS NOT NULL
				GROUP BY
					TABLE_NAME, TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_TABLE_SCHEMA
			""".format(columnseparator, self.db_catalog))
			retval = [ ForeignKey(db_catalog=d[1], pkdb_schema='', fkdb_schema='', pktablename=d[3], fktablename=d[4], keyname=d[5], pk_columns=d[6].split(columnseparator), fk_columns=d[7].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
		return retval