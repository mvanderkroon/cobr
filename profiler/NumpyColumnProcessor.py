import sys
import numpy as np
import math

class NumpyColumnProcessor():
	def __init__(self, data=[]):
		self.data = data

	def min(self, data):
		return np.min(data).item()

	def max(self, data):
		return np.max(data).item()

	def mean(self, data):
		return np.mean(data).item()

	def var(self, data):
		return np.var(data).item()

	def median(self, data):
		return np.median(data).item()

	def std(self, data):
		return np.std(data).item()

	def sum(self, data):
		return np.sum(data).item()

	def mode(self, data):
		c = Counter(data)
		return c.most_common(1)[0][0]

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
		return None

	def endDate(self, data):
		return None

	def lifeSpan(self, data):
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

				if (retdict[operation['name']] is not None and math.isnan(retdict[operation['name']])):
					retdict[operation['name']] = None

			except Exception as ex:
				retdict[operation['name']] = None

		return retdict