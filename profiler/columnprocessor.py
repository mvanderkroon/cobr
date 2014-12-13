from objects import ForeignKey, PrimaryKey, Table, Column
import pymssql
import datetime
import math
import numpy as np
from collections import Counter

class NumpyColumnProcessor():
	def __init__(self, data=[], column=None):
		self.data = data
		self.column = column

	def min(self, data):
		try:
			return float(np.min(data))
		except:
			return None

	def max(self, data):
		try:
			return float(np.max(data))
		except:
			return None

	def mean(self, data):
		try:
			return float(np.mean(data))
		except:
			return None

	def var(self, data):
		try:
			return float(np.var(data))
		except:
			return None

	def median(self, data):
		try:
			return float(np.median(data))
		except:
			return None

	def std(self, data):
		try:
			return float(np.std(data))
		except:
			return None

	def sum(self, data):
		try:
			return float(np.sum(data))
		except:
			return None

	def mode(self, data):
		try:
			c = Counter(data)
			return float(c.most_common(1)[0][0])
		except:
			return None

	def positiveCount(self, data):
		try:
			retval = 0
			for d in data:
				if d > 0:
					retval += 1
			return retval
		except:
			return 0

	def negativeCount(self, data):
		try:
			retval = 0
			for d in data:
				if d < 0:
					retval += 1
			return retval
		except:
			return 0

	def zeroCount(self, data):
		try:
			retval = 0
			for d in data:
				if d == 0:
					retval += 1
			return retval
		except:
			return 0

	def uniqueCount(self, data):
		try:
			c = Counter(data)
			return len(c)
		except Exception as ex:
			return 0

	def nullCount(self, data):
		try:
			retval = 0
			for d in data:
				if d is None:
					retval += 1
			return retval
		except Exception as ex:
			return 0

	def startDate(self, data):
		try:
			return None
		except:
			return None

	def endDate(self, data):
		try:
			return None
		except:
			return None

	def lifeSpan(self, data):
		pass
		try:
			return datetime.datetime(np.max(data)) - datetime.datetime(np.min(data))
		except:
			return None

	def doOperations(self):
		operations = [
				{ 'op':self.min, 'name':'min' },
				{ 'op':self.max, 'name':'max' },
				{ 'op':self.mean, 'name':'avg' }, 
				{ 'op':self.var, 'name':'var' }, 
				{ 'op':self.median, 'name':'median' }, 
				{ 'op':self.std, 'name':'stdev' }, 
				{ 'op':self.sum, 'name':'sum' },
				{ 'op':self.mode, 'name':'mode' }, 
				{ 'op':self.lifeSpan, 'name':'lifespan_in_days' }, 
				{ 'op':self.positiveCount, 'name':'num_positive' }, 
				{ 'op':self.negativeCount, 'name':'num_negative' }, 
				{ 'op':self.zeroCount, 'name':'num_zero' }, 
				{ 'op':self.uniqueCount, 'name':'num_distinct_values' }, 
				{ 'op':self.nullCount, 'name':'num_nulls' },
				{ 'op':self.startDate, 'name':'start_date' },
				{ 'op':self.endDate, 'name':'end_date' }
			]

		retdict = {}
		for operation in operations:
			try:
				retdict[operation['name']] = operation['op'](self.data)

				if (math.isnan(retdict[operation['name']])):
					retdict[operation['name']] = None

				# SQLalchemy does not play well with numpy data types, as such we cast those types back to python types
				# if 'numpy.int64' in str(type(retdict[operation['name']])):
				# 	retdict[operation['name']] = int(retdict[operation['name']])
				# if 'numpy.float64' in str(type(retdict[operation['name']])):
				# 	retdict[operation['name']] = float(retdict[operation['name']])
				# if 'uuid.UUID' in str(type(retdict[operation['name']])):
				# 	retdict[operation['name']] = str(retdict[operation['name']])
				# if 'numpy.bool_' in str(type(retdict[operation['name']])):
				# 	retdict[operation['name']] = bool(retdict[operation['name']])
			except Exception as ex:
				retdict[operation['name']] = None
		return retdict

class MssqlColumnProcessor():
	def __init__(self, db_host='127.0.0.1', db_user='sa', db_password='', column=None):
		self.column = column
		self.db_host = db_host
		self.db_user = db_user
		self.db_password = db_password

	def getMinMaxAvgStdevSumVar(self):
		retval = {
			'min': float('nan'),
			'max': float('nan'),
			'avg': float('nan'),
			'stdev': float('nan'),
			'sum': float('nan'),
			'var': float('nan'),
		}

		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor(as_dict=True) as cursor:
				cursor.execute("""
					SELECT 
						MIN(CAST([{3}] AS FLOAT)) as min, MAX(CAST([{3}] AS FLOAT)) as max, AVG(CAST([{3}] AS FLOAT)) as avg, STDEV(CAST([{3}] AS FLOAT)) as stdev, SUM(CAST([{3}] AS FLOAT)) as sum, VAR(CAST([{3}] AS FLOAT)) as var
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				dic =  cursor.fetchone()
				retval['min'] = dic['min']
				retval['max'] = dic['max']
				retval['avg'] = dic['avg']
				retval['stdev'] = dic['stdev']
				retval['sum'] = dic['sum']
				retval['var'] = dic['var']
		return retval


	def getMin(self):
		retval = float('nan')

		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval
		
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						MIN(CAST([{3}] AS FLOAT))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval =  cursor.fetchone()[0]
		return retval

	def getMax(self):
		retval = float('nan')

		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval
		
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						MAX(CAST([{3}] AS FLOAT))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval =  cursor.fetchone()[0]
		return retval

	def getAvg(self):
		retval = float('nan')

		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						AVG(CAST([{3}] AS FLOAT))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getStdev(self):
		retval = float('nan')

		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						STDEV(CAST([{3}] AS FLOAT))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getSum(self):
		retval = float('nan')
		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						SUM(CAST([{3}] AS FLOAT))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getMedian(self):
		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return float('nan') 
		
		data = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						CAST([{3}] AS FLOAT)
					FROM
						[{0}].[{1}].[{2}]
					ORDER BY
						CAST([{3}] AS FLOAT) DESC
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				try:
					data = [d[0] for d in cursor.fetchall()]
				except:
					pass
		
		l = len(data)
		if l > 0:
			if l % 2 == 0: # even
				ml = int(l/2)
				mu = int(l/2) + 1
				return (float(data[ml]) + float(data[mu])) / 2
			else: # odd
				return float(data[int(l/2)])
		else: # empty column?
			return float('nan')

	def getVariance(self):
		retval = float('nan')

		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						VAR(CAST([{3}] AS FLOAT))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getMode(self):
		retval = float('nan')
		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT
						TOP 1 CAST([{3}] AS FLOAT), COUNT(*)
					FROM
						[{0}].[{1}].[{2}]
					GROUP BY
						CAST([{3}] AS FLOAT)
					ORDER BY
						COUNT(*) DESC
					""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				try:
					retval = cursor.fetchone()[0]
				except:
					pass
		return retval

	def getStartDate(self):
		retval = None
		allowed_datatypes = ['datetime', 'date', 'time', 'datetime2', 'smalldatetime']
		if self.column.datatype not in allowed_datatypes:
			return retval
		
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						MIN([{3}])
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getEndDate(self):
		retval = None
		allowed_datatypes = ['datetime', 'date', 'time', 'datetime2', 'smalldatetime']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						MAX([{3}])
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getLifeSpan(self):
		retval = float('nan')
		allowed_datatypes = ['datetime', 'date', 'time', 'datetime2', 'smalldatetime']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT 
						DATEDIFF(DAY, MIN([{3}]), MAX([{3}]))
					FROM
						[{0}].[{1}].[{2}]
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getPositiveCount(self):		
		retval = float('nan')
		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT
						COUNT([{3}])
					FROM
						[{0}].[{1}].[{2}]
					WHERE
						[{3}] > 0
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getNegativeCount(self):
		retval = float('nan')
		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT
						COUNT([{3}])
					FROM
						[{0}].[{1}].[{2}]
					WHERE
						[{3}] < 0
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getZeroCount(self):
		retval = float('nan')
		allowed_datatypes = ['int', 'float', 'decimal', 'numeric', 'bigint', 'money', 'smallint', 'smallmoney', 'tinyint', 'real']
		if self.column.datatype not in allowed_datatypes:
			return retval

		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT
						COUNT([{3}])
					FROM
						[{0}].[{1}].[{2}]
					WHERE
						[{3}] = 0
						""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getNullCountFor(self):
		retval = float('nan')
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT
						COUNT(*)
					FROM
						[{0}].[{1}].[{2}]
					WHERE [{3}] IS NULL
					""".format(self.column.db_catalog, self.column.db_schema, self.column.tablename, self.column.columnname))
				retval = cursor.fetchone()[0]
		return retval

	def getDistinctValuesFor(self):
		tdict = {}
		tdict['image'] = "CAST(CAST([{0}] AS BINARY) AS NVARCHAR(MAX))"
		tdict['text'] = "CAST([{0}] AS NVARCHAR(MAX))"
		tdict['ntext'] = "CAST([{0}] AS NVARCHAR(MAX))"

		selector = "[{0}]".format(self.column.columnname)
		if self.column.datatype in tdict:
			selector = tdict[self.column.datatype].format(self.column.columnname)

		data = []
		with pymssql.connect(self.db_host, self.db_user, self.db_password, self.column.db_catalog) as conn:
			with conn.cursor() as cursor:
				cursor.execute("""
					SELECT
						{0}, COUNT(*)
					FROM
						[{1}].[{2}].[{3}]
					GROUP BY
						{0}
					ORDER BY
						COUNT(*) DESC
					""".format(selector, self.column.db_catalog, self.column.db_schema, self.column.tablename))
				data = [ { 'value': d[0], 'count': d[1] } for d in cursor.fetchall() ]
		return data