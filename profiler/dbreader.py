from objects import ForeignKey, PrimaryKey, Table, Column

import pymssql
import pymysql

class Factory():
	def __init__(self, type='pymssql'):
		self.type = type

		self.classes = {
			'pymssql': mssqlMiner,
			'pymysql': mysqlMiner
		}

	def getInstance(self, db_catalog='', db_host='127.0.0.1', db_user='', db_password=''):	
		return self.classes[self.type](db_catalog=db_catalog, db_host=db_host, db_user=db_user, db_password=db_password)

class mysqlMiner():

	def __init__(self, db_catalog='', db_host='127.0.0.1', db_user='', db_password=''):
		self.db_catalog = db_catalog
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getDataForColumn(self, column=None, verbose=False):
		if column is None:
			return None

		selectclause = "`{0}`".format(column.columnname)		
		
		retval = []
		with pymysql.connect(host=self.db_host, user=self.db_user, passwd=self.db_password, db=self.db_catalog) as cursor:
			query = """
				SELECT 
					{0}
				FROM 
					`{1}`.`{2}`
				ORDER BY {0} ASC
				""".format(selectclause, column.db_schema, column.tablename)
			
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
			retval = [ Table(db_catalog=d[0], db_schema=d[1], tablename=d[2]) for d in cursor.fetchall() ]
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
			retval = [ Column(db_catalog=d[0], db_schema=d[1], tablename=d[2], columnname=d[3], datatype=d[4], ordinal_position=d[5]) for d in cursor.fetchall() ]
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
			retval = [ PrimaryKey(db_catalog=d[0], db_schema=d[1], tablename=d[2], keyname='expk_' + str(d[2]), db_columns=d[3].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
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
			retval = [ ForeignKey(db_catalog=d[0], pkdb_schema=d[1], fkdb_schema=d[2], pktablename=d[3], fktablename=d[4], keyname=d[5], pk_columns=d[6].split(columnseparator), fk_columns=d[7].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
		return retval


class mssqlMiner():

	def __init__(self, db_catalog='', db_host='127.0.0.1', db_user='', db_password=''):
		self.db_catalog = db_catalog
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getDataForColumn(self, column=None, verbose=False):
		if column is None:
			return None

		# print('getting data for: {0}.{1} -- {2} '.format(column.db_schema, column.tablename, column.columnname))

		tdict = {}
		tdict['image'] = "CAST(CAST([{0}] AS BINARY) AS NVARCHAR(MAX))"
		tdict['text'] = "CAST([{0}] AS NVARCHAR(MAX))"
		tdict['ntext'] = "CAST([{0}] AS NVARCHAR(MAX))"

		selectclause = "[{0}]".format(column.columnname)		
		if column.datatype in tdict:
			selectclause = tdict[column.datatype].format(column.columnname)
		
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				query = """
					SELECT 
						{0}
					FROM 
						[{1}].[{2}]
					ORDER BY {0} ASC
					""".format(selectclause, column.db_schema, column.tablename)
				
				if verbose:
					print(query)
					print('')

				cursor.execute(query)
				retval = [ d[0] for d in cursor.fetchall() ]

		return retval;

	def getTables(self):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
					FROM
						INFORMATION_SCHEMA.TABLES 
					WHERE
						TABLE_CATALOG = '{0}' AND TABLE_TYPE = 'BASE TABLE' 
				""".format(self.db_catalog))
				retval = [ Table(db_catalog=d[0], db_schema=d[1], tablename=d[2]) for d in cursor.fetchall() ]
		return retval

	def getColumns(self):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
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
						T.TABLE_CATALOG = '{0}' AND T.TABLE_TYPE = 'BASE TABLE' 
					""".format(self.db_catalog))
				retval = [ Column(db_catalog=d[0], db_schema=d[1], tablename=d[2], columnname=d[3], datatype=d[4], ordinal_position=d[5]) for d in cursor.fetchall() ]
		return retval

	def getPrimaryKeys(self, columnseparator='|'):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					with cte1 as (
						SELECT
							Tab.TABLE_CATALOG,
							Tab.TABLE_SCHEMA,
							Tab.TABLE_NAME,
							Col.CONSTRAINT_NAME,
							Col.COLUMN_NAME 
						FROM 
							INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
							INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
						WHERE 
							Col.Constraint_Name = Tab.Constraint_Name
							AND Col.Table_Name = Tab.Table_Name
							AND Constraint_Type = 'PRIMARY KEY'
					)    

					SELECT
						G.TABLE_CATALOG,
						G.TABLE_SCHEMA,
						G.TABLE_NAME,
						G.CONSTRAINT_NAME,
						stuff(
						(
							SELECT cast('{0}' as varchar(max)) + U.COLUMN_NAME
							FROM cte1 U
							WHERE U.CONSTRAINT_NAME = G.CONSTRAINT_NAME and U.TABLE_CATALOG = G.TABLE_CATALOG and U.TABLE_SCHEMA = G.TABLE_SCHEMA and U.TABLE_NAME = G.TABLE_NAME
							for xml path('')
						), 1, 1, '') AS PKCOLS
					FROM
						cte1 G
					WHERE
						G.TABLE_CATALOG = '{1}'
					GROUP BY
						G.CONSTRAINT_NAME, G.TABLE_CATALOG, G.TABLE_SCHEMA, G.TABLE_NAME
				""".format(columnseparator, self.db_catalog))
				retval = [ PrimaryKey(db_catalog=d[0], db_schema=d[1], tablename=d[2], keyname=d[3], db_columns=d[4].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
		return retval

	def getForeignKeys(self, columnseparator='|'):
		retval = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute(""" 
					with cte1 as (
						SELECT  
							obj.name AS FK_NAME,
							pksch.name AS [pkschema],
							fksch.name AS [fkschema],
							tab1.name AS [table],
							col1.name AS [column],
							tab2.name AS [referenced_table],
							col2.name AS [referenced_column]
						FROM sys.foreign_key_columns fkc
						INNER JOIN sys.objects obj
							ON obj.object_id = fkc.constraint_object_id
						INNER JOIN sys.tables tab1
							ON tab1.object_id = fkc.parent_object_id
						INNER JOIN sys.tables tab2
							ON tab2.object_id = fkc.referenced_object_id
						INNER JOIN sys.schemas pksch
							ON tab1.schema_id = pksch.schema_id
						INNER JOIN sys.schemas fksch
							ON tab2.schema_id = fksch.schema_id
						INNER JOIN sys.columns col1
							ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
						INNER JOIN sys.columns col2
							ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
					)

					SELECT
						G.pkschema,
						G.fkschema,
						G.[table] as pktable,
						G.referenced_table as fktable,
						G.FK_NAME,
						stuff(
						(
							select cast('{0}' as varchar(max)) + U.[column]
							from cte1 U
							WHERE U.FK_NAME = G.FK_NAME
							for xml path('')
						), 1, 1, '') AS pkcolumns,
						stuff(
						(
							select cast('{0}' as varchar(max)) + U.[referenced_column]
							from cte1 U
							WHERE U.FK_NAME = G.FK_NAME
							for xml path('')
						), 1, 1, '') AS fkcolumns
					FROM
						cte1 G
					GROUP BY
						G.FK_NAME, G.pkschema, G.fkschema, G.[table], G.referenced_table
				 """.format(columnseparator))
				retval = [ ForeignKey(db_catalog=self.db_catalog, pkdb_schema=d[0], fkdb_schema=d[1], pktablename=d[2], fktablename=d[3], keyname=d[4], pk_columns=d[5].split(columnseparator), fk_columns=d[6].split(columnseparator), type='explicit') for d in cursor.fetchall() ]
		return retval

def main():
	pass

if __name__ == '__main__':
	main()