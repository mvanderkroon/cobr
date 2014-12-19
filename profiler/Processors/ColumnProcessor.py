import sys

class ColumnProcessor():
	def __init__(self, columns=[], getDataFn=None, processor=None):
		self.columns = columns
		self.getDataFn = getDataFn
		self.processor = processor

	def execute(self):
		print('')
		print('## columnprocessor started')
		print('\n'*3)
		for i,column in enumerate(self.columns):
			try:
				sys.stdout.write("\033[3A")

				totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(self.columns)-1, round(i/(len(self.columns)-1)*100,2))
				sys.stdout.write(totalprogress)
				sys.stdout.flush()
				currenttab = "\r\033[K## table: {0}.{1}.{2} \n".format(column.db_catalog, column.db_schema, column.tablename)
				sys.stdout.write(currenttab)
				sys.stdout.flush()
				currentcol = "\r\033[K## column: {0} \n".format(column.columnname)
				sys.stdout.write(currentcol)
				sys.stdout.flush()
				
				# print('processing {0} of {1:5}: {2}.{3}.{4}.{5}'.format(i+1, len(columns), column.db_catalog, column.db_schema, column.tablename, column.columnname))
				columnProcessor = self.processor(data=self.getDataFn(column), column=column)
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
			except Exception as e:
				print('failed to process: {0}.{1}.{2}.{3}'.format(column.db_catalog, column.db_schema, column.tablename, column.columnname))
				print(e)
		print('##')
		print('## columnprocessor done')
		print('##')
		