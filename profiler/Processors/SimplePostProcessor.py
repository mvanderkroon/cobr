class SimplePostProcessor():
	def __init__(self, tables=[], columns=[], explicit_foreignkeys=[], explicit_primarykeys=[], implicit_foreignkeys=[], implicit_primarykeys=[]):
		self.tables = tables
		self.columns = columns
		self.explicit_foreignkeys = explicit_foreignkeys
		self.explicit_primarykeys = explicit_primarykeys
		self.implicit_foreignkeys = implicit_foreignkeys
		self.implicit_primarykeys = implicit_primarykeys

	def getNumExplicitInlinksForTable(self, table):
		explicit_inlinks = []
		for efk in self.explicit_foreignkeys:
			if efk.fktablename == table.tablename and efk.db_catalog == table.db_catalog and efk.fkdb_schema == table.db_schema:
				explicit_inlinks.append(efk)
		return explicit_inlinks

	def getNumExplicitOutlinksForTable(self, table):
		explicit_outlinks = []
		for efk in self.explicit_foreignkeys:
			if efk.pktablename == table.tablename and efk.db_catalog == table.db_catalog and efk.pkdb_schema == table.db_schema:
				explicit_outlinks.append(efk)
		return explicit_outlinks

	def getNumImplicitInlinksForTable(self, table):
		pass

	def getNumImplicitOutlinksForTable(self, table):
		pass

	def getTableFillScore(self, table):
		try:
			num_rows = table.num_rows

			sum = 0
			numcols = 0
			for column in self.columns:
				if column.tablename == table.tablename and column.db_catalog == table.db_catalog and column.db_schema == table.db_schema:
					if column.num_nulls is None:
						sum += 0
					else:
						sum += column.num_nulls
					numcols += 1
			if (numcols*num_rows) > 0:
				return float(sum) / float(numcols*num_rows)
			else:
				return 0
		except:
			return None

	def getTableNullColumns(self, table):
		try:
			num_rows = table.num_rows
			result = []
			for column in self.columns:
				if column.tablename == table.tablename and column.db_catalog == table.db_catalog and column.db_schema == table.db_schema and column.num_nulls == num_rows:
					result.append(column)
			return result
		except:
			return None