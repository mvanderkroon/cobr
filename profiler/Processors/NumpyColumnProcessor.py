import sys
sys.path.append("../../common")

from objects import ForeignKey, PrimaryKey, Table, Column

import numpy
import math

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

			except Exception as ex:
				retdict[operation['name']] = None
		return retdict