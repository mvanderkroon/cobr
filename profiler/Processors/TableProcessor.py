import sys

class TableProcessor():
	def __init__(self, tables=[], processor=None):
		self.tables = tables
		self.processor = processor

	def execute(self):
		print('')
		print('## tableprocessor started')
		print('\n'*2)
		for i,table in enumerate(self.tables):
			try:
				sys.stdout.write("\033[2A")

				totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(self.tables)-1, round(i/(len(self.tables)-1)*100,2))
				sys.stdout.write(totalprogress)
				sys.stdout.flush()
				currenttab = "\r\033[K## table: {0}.{1}.{2} \n".format(table.db_catalog, table.db_schema, table.tablename)
				sys.stdout.write(currenttab)
				sys.stdout.flush()
				
				table.num_rows = self.processor.getTableRowCount(table=table)
				table.num_columns = self.processor.getColumnCountForTable(table=table)
			except Exception as e:
				print('failed to process: {0}.{1}.{2}'.format(table.db_catalog, table.db_schema, table.tablename))
				print(e)
		print('##')
		print('## tableprocessor done')
		print('##')