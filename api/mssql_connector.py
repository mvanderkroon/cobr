import sys
sys.path.append("../common")

import pymssql

class database():
	
	def __init__(self, db_host='10.0.0.127', db_user='', db_password='', db_catalog=''):
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password
		self.db_catalog = db_catalog

		self.conn = pymssql.connect(db_host, db_user, db_password, db_catalog)
		self.cur = self.conn.cursor()

		self.coldict = {}
		for column in self.getColumns():
			self.coldict[(column['table_schema'], column['table_name'], column['column_name'])] = column
	
	def getTables(self):
		# get tables
		self.cur.execute("""
			select 
				TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
			from 
				information_schema.tables 
			where 
				table_type = 'base table' 
		""")
		tables = []
		for row in self.cur:
			table = { 'table_catalog': str(row[0]), 'table_schema': str(row[1]), 'table_name': str(row[2])}
			tables.append(table)

		return tables

	def getColumns(self):
		# get columns
		self.cur.execute("""
			select 
				C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.ORDINAL_POSITION, C.DATA_TYPE, T.TABLE_CATALOG
			from 
				INFORMATION_SCHEMA.COLUMNS C
			join
				INFORMATION_SCHEMA.TABLES T
			on
				C.TABLE_CATALOG = T.TABLE_CATALOG and C.TABLE_SCHEMA = T.TABLE_SCHEMA and C.TABLE_NAME = T.TABLE_NAME
			where 
				T.TABLE_TYPE = 'base table'
			""")
		columns = []
		for row in self.cur:
			column = { 'table_schema': str(row[0]), 'table_name': str(row[1]), 'column_name': str(row[2]), 'data_type': str(row[4]), 'table_catalog': str(row[5]) }
			columns.append(column)
		return columns

	def getSingleImplicitPK(self):
		self.cur.execute("""
		SELECT
			[database],pkname,pktable,pkcolumn,schemaname,id,score,type
		FROM
			meta.dbo.mprimarykey
		WHERE
			type = 'implicit'
		AND
			pkcolumn NOT LIKE '%|%'
		""")
		pksingle = []
		for row in self.cur:
			colnames = str(row[3]).split('|')
			pk = { 'pkname': str(row[1]), 'table_catalog': str(row[0]), 'table_schema': str(row[4]), 'table_name': str(row[2]), 'column_name': str(row[3]), 'columns': str(row[3]).split('|') }
			pksingle.append(pk)
		return pksingle

	def getMultiImplicitPK(self):
		self.cur.execute("""
		SELECT
			[database],pkname,pktable,pkcolumn,schemaname,id,score,type
		FROM
			meta.dbo.mprimarykey
		WHERE
			type = 'implicit'
		AND
			pkcolumn LIKE '%|%'
		""")
		pkmulti = []
		for row in self.cur:
			colnames = str(row[3]).split('|')
			pk = { 'pkname': str(row[1]), 'table_catalog': str(row[0]), 'table_schema': str(row[4]), 'table_name': str(row[2]), 'column_name': str(row[3]), 'columns': str(row[3]).split('|') }
			pkmulti.append(pk)
		return pkmulti

	def getSinglePK(self):
		# singlecolumn primary keys
		self.cur.execute("""
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
				G.CONSTRAINT_NAME,
				G.TABLE_CATALOG,
				G.TABLE_SCHEMA,
				G.TABLE_NAME,
				stuff(
				(
					select cast('|' as varchar(max)) + U.COLUMN_NAME
					from cte1 U
					WHERE U.CONSTRAINT_NAME = G.CONSTRAINT_NAME and U.TABLE_CATALOG = G.TABLE_CATALOG and U.TABLE_SCHEMA = G.TABLE_SCHEMA and U.TABLE_NAME = G.TABLE_NAME
					for xml path('')
				), 1, 1, '') AS PKCOLS
			FROM
				cte1 G
			GROUP BY
				G.CONSTRAINT_NAME, G.TABLE_CATALOG, G.TABLE_SCHEMA, G.TABLE_NAME
			HAVING
				COUNT(*) = 1
		""")
		pksingle = []
		for row in self.cur:
			colnames = str(row[4]).split('|')
			pk = { 'pkname': str(row[0]), 'table_catalog': str(row[1]), 'table_schema': str(row[2]), 'table_name': str(row[3]), 'column_name': str(row[4]), 'columns': str(row[4]).split('|') }
			pksingle.append(pk)
		return pksingle

	def getMultiPK(self):
		# multicolumn primary keys
		self.cur.execute("""
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
				G.CONSTRAINT_NAME,
				G.TABLE_CATALOG,
				G.TABLE_SCHEMA,
				G.TABLE_NAME,
				stuff(
				(
					select cast('|' as varchar(max)) + U.COLUMN_NAME
					from cte1 U
					WHERE U.CONSTRAINT_NAME = G.CONSTRAINT_NAME and U.TABLE_CATALOG = G.TABLE_CATALOG and U.TABLE_SCHEMA = G.TABLE_SCHEMA and U.TABLE_NAME = G.TABLE_NAME
					for xml path('')
				), 1, 1, '') AS PKCOLS
			FROM
				cte1 G
			GROUP BY
				G.CONSTRAINT_NAME, G.TABLE_CATALOG, G.TABLE_SCHEMA, G.TABLE_NAME
			HAVING
				COUNT(*) > 1
		""")
		pkmulti = []
		for row in self.cur:
			pk = { 'pkname': str(row[0]), 'table_catalog': str(row[1]), 'table_schema': str(row[2]), 'table_name': str(row[3]), 'column_name': str(row[4]), 'columns': str(row[4]).split('|') }
			pkmulti.append(pk)
		return pkmulti

	def doDataInit(self):
		self.coldict = {}
		for column in self.getColumns():
			self.coldict[(column['table_schema'], column['table_name'], column['column_name'])] = column

	def getData(self, schema, columnnames, tablename):
		print('getting data for: {0}.{1} -- {2} '.format(schema, tablename, str(columnnames)))

		tdict = {}
		tdict['image'] = "CAST(CAST([{0}] AS BINARY) AS NVARCHAR(MAX))"
		tdict['text'] = "CAST([{0}] AS NVARCHAR(MAX))"
		tdict['ntext'] = "CAST([{0}] AS NVARCHAR(MAX))"

		selectclause = ""
		if type(columnnames) is list:
			# first, figure out column datatype
			for cn in columnnames:
				key = (schema, tablename, cn)

				if self.coldict[key]['data_type'] in tdict:
					selectclause += tdict[self.coldict[key]['data_type']].format(cn) + ","
				else:
					selectclause += "[{0}],".format(cn)
			selectclause = selectclause[0:-1]
		else:
			# first, figure out column datatype
			key = (schema, tablename, str(columnnames))
			if self.coldict[key]['data_type'] in tdict:
				selectclause += tdict[self.coldict[key]['data_type']].format(str(columnnames))
			else:
				selectclause += "[{0}]".format(columnnames)

		result = []

		q = """
			SELECT 
				{0}, COUNT(*)
			FROM 
				[{1}].[{2}]
			GROUP BY
				{0}
			ORDER BY COUNT(*) DESC
			""".format(selectclause, schema, tablename)
		
		print(q)
		print('')

		self.cur.execute(q)
		columns = []
		for row in self.cur:
			if len(columnnames) > 1:
				result.append(list(row))
			else:
				result.append(list(row))
				
		return result;

	def close(self):
		self.cur.close()
		self.conn.close()
