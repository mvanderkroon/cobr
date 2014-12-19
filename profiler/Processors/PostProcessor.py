class PostProcessor():
	def __init__(self, tables=[], columns=[], explicit_foreignkeys=[], explicit_primarykeys=[], implicit_foreignkeys=[], implicit_primarykeys=[], processor=None):
		self.tables = tables
		self.columns = columns
		self.explicit_foreignkeys = explicit_foreignkeys
		self.explicit_primarykeys = explicit_primarykeys
		self.implicit_foreignkeys = implicit_foreignkeys
		self.implicit_primarykeys = implicit_primarykeys
		self.processor = processor

	def execute(self):
		postprocessor = processor(tables=self.tables, columns=self.columns, explicit_foreignkeys=self.explicit_foreignkeys, explicit_primarykeys=self.explicit_primarykeys, implicit_foreignkeys=self.implicit_foreignkeys, implicit_primarykeys=self.implicit_primarykeys)
		for i,table in enumerate(self.tables):
			try:
				#print('processing {0} of {1:5}: {2}.{3}.{4}'.format(i+1, len(tables), table.db_catalog, table.db_schema, table.tablename))
				
				table.num_explicit_inlinks = len(postprocessor.getNumExplicitInlinksForTable(table))
				table.num_explicit_outlinks = len(postprocessor.getNumExplicitOutlinksForTable(table))				
				table.num_emptycolumns = len(postprocessor.getTableNullColumns(table))
				table.tablefillscore = postprocessor.getTableFillScore(table)
			except Error as e:
				print('failed to process: {0}.{1}.{2}'.format(table.db_catalog, table.db_schema, table.tablename))
				print(e)

