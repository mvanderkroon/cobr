import sys
sys.path.append("../common")

from objects import ForeignKey, PrimaryKey, Table, Column
import numpy as np

import pymssql
import pymysql

class MysqlTableProcessor():
	def __init__(self, db_host='127.0.0.1', db_user='sa', db_password='', table=None):
		self.table = table
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getTableRowCount(self):
		retval = float('nan')
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.table.db_schema) as cursor:
			cursor.execute(""" 
				SELECT 
					COUNT(*) 
				FROM `{0}`.`{1}`""".format(self.table.db_schema, self.table.tablename))
			retval = cursor.fetchone()[0]
		return retval

	def getColumnCountForTable(self):
		retval = float('nan')
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.table.db_schema) as cursor:
			cursor.execute(""" 
				SELECT 
					COUNT(*)
				FROM 
					INFORMATION_SCHEMA.COLUMNS 
				WHERE 
					TABLE_CATALOG = '{0}' 
				AND 
					TABLE_SCHEMA = '{1}' 
				AND 
					TABLE_NAME = '{2}'""".format(self.table.db_catalog, self.table.db_schema, self.table.tablename))
			retval = cursor.fetchone()[0]
		return retval

	def getImplicitForeignKeys(self):
		return None

	def getImplicitPrimaryKeys(self):
		return None

class MssqlTableProcessor():
	def __init__(self, db_host='127.0.0.1', db_user='sa', db_password='', table=None):
		self.table = table
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getTableRowCount(self):
		retval = float('nan')
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.table.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(""" 
					SELECT 
						COUNT(*) 
					FROM [{0}].[{1}].[{2}]""".format(self.table.db_catalog, self.table.db_schema, self.table.tablename))
				retval = cursor.fetchone()[0]
		return retval

	def getColumnCountForTable(self):
		retval = float('nan')
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.table.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(""" 
					SELECT 
						COUNT(*)
					FROM 
						INFORMATION_SCHEMA.COLUMNS 
					WHERE 
						TABLE_CATALOG = '{0}' 
					AND 
						TABLE_SCHEMA = '{1}' 
					AND 
						TABLE_NAME = '{2}'""".format(self.table.db_catalog, self.table.db_schema, self.table.tablename))
				retval = cursor.fetchone()[0]
		return retval

	def getImplicitForeignKeys(self):
		return None

	def getImplicitPrimaryKeys(self):
		return None

def main():
	pass

if __name__ == '__main__':
	main()





































