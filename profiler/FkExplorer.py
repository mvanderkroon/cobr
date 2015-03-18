import sys, os, math, emd, os, argparse
sys.path.append("../common")
sys.path.append("../api")

from MetaModel import MetaModel
from objects import ForeignKey, PrimaryKey, Table, Column, Base

import numpy as np
from sqlalchemy import and_

class Discovery():
	def __init__(self, tables=[], columns=[], pksingle=[], pkmulti=[], colseparator='|'):
		self.tables = tables
		self.columns = columns
		self.pksingle = pksingle
		self.pkmulti = pkmulti
		self.colseparator = colseparator

	def cartesian(self, L):
		if (len(L) == 0 or type(L) != list): # this shouldn't fucking happen
			return L
		elif len(L) == 1: # the base case 1
			return L[0]
		else: # now it boils down to a pair product (and a recursive call)
			return [(x if type(x)==list else [x]) + (y if type(y)==list else [y]) for x in L[0] for y in self.cartesian(L[1:])]

	def bottomksketch(self, values, k=512):
		hashes = []
		for value in values:
			value = ''.join(str(value))
			hashes.append(hash(value))
		l = len(hashes)
		return sorted(hashes)[l-k:l]

	def inclusion(self, a, b):
		if len(a) == 0 or len(b) == 0: # apparently one list of values is empty, so inclusion should always be zero
			# print('empty!!')
			return 0

		a = set(a)
		b = set(b)

		intersection = a.intersection(b)
		union = a.union(b)

		return len(intersection) / len(union)

	def quantilehistogram(self, values, numbins=256):
		lists = [list(t) for t in zip(*values)] # unpack pairs of values into a list of lists

		if len(lists) == 0: # empty column...
			return None

		hists = []
		for l in lists:
			binsize = math.sqrt(len(l))
			if binsize >= 500:
				binsize = 499

			hist = []
			bins = []
			try:
				# print('trying as is..')
				sum(l)
				hist, bins = np.histogram(l, bins=binsize, density=True) # sqrt to improve accuracy for larger tables
			except:
				try:
					# print('trying to cast to ints..')
					castlist = [ int(value) for value in l ]
					hist, bins = np.histogram(castlist, bins=binsize, density=True)
				except:
					# print('trying as is hashed..')
					hashedlist = [ hash(value) for value in l ]
					hist, bins = np.histogram(hashedlist, bins=binsize, density=True)
				# c = collections.Counter(l)
				# rhist = list(map((lambda x: x/len(l)), list(c.values()))) # for each quantile (map) divide by total number of records to get probability
				# rbins = list(c)

				# for i in range(numbins):
				# 	if i < len(rhist) and i < len(rbins):
				# 		hist.append(rbins[i])
				# 		bins.append(rhist[i])
				# 	else:
				# 		hist.append(0)
				# 		bins.append(0)
				# bins.append(0)
			hists.append((list(hist), list(bins)))

		return hists

	def colKey(column):
		return (column.info['schemaname'], column.table, column.name)

	def pkKey(pk):
		return (pk['schemaname'], pk['tablename'], colseparator.join(pk['constrained_columns']))

	def discoverfks(self, theta):
		# phase 1
		fs = []
		fm = []
		# b will contain bottom-k sketches for each column, indexed on (<schemaname>, <tablename>, <columnname>)
		bksketches = {}
		quantiles = {}
		s = {}

		# calculate bottom-k sketch for all columns and store in dictionary <bksketches>
		for column in self.columns:
			bksketches[(column.db_schema, column.tablename, column.columnname)] = self.bottomksketch(self.getDataFn(column.db_schema, column.columnname, column.tablename))

		pkall = self.pksingle
		pkall.extend(self.pkmulti)
		for pk in pkall: # foreach primary key (single and multi)
			pkcollst = pk.db_columns.split(self.colseparator)
			n = len(pkcollst)

			for keycolumn_name in pkcollst: # foreach column in primary key
				for candidate in self.columns: # foreach column as foreign key candidate
					this = bksketches[(candidate.db_schema, candidate.tablename, candidate.columnname)]
					that = bksketches[(pk.db_schema, pk.tablename, keycolumn_name)]
					if self.inclusion(this, that) >= theta and (candidate.tablename != pk.tablename):
						if n == 1: # in case we are dealing with a single column pk
							fs.append(([candidate], pk))
						if n > 1: # in case we are dealing with a multi column pk
							if (pk.db_columns, keycolumn_name) not in s:
								s[(pk.db_columns, keycolumn_name)] = []
							# dictionary s indexes on (<pk name>, <pk column>) where the pk name is generic (can be
							# just concatenation of the columnnames), e.g.: ('id|name', 'id') and ('id|name', 'name')
							# indicate the two entries in s for PK 'id|name'. For each entry we store a list of
							# candidate columns found in other tables
							s[(pk.db_columns, keycolumn_name)].append(candidate)
			if n > 1:
				bksketches[(pk.db_schema, pk.tablename, pk.db_columns)] = self.bottomksketch(self.getDataFn(pk.db_schema, pk.db_columns.split(self.colseparator), pk.tablename))

			quantiles[(pk.db_schema, pk.tablename, pk.db_columns)] = self.quantilehistogram(self.getDataFn(pk.db_schema, pk.db_columns.split(self.colseparator), pk.tablename))

		# phase 2

		# fks: dictionary that indexes on (<foreignkey table>, <primary key column>)
		# value of the dictionary are those candidate columns in <foreignkey table> for <primary key column>
		# TBD: remove the table loop
		fks = {}
		for kvp in s:
			spkcolname = kvp[1]
			for e in s[kvp]:
				key = (e.tablename, spkcolname)
				if key not in fks:
					fks[key] = []
				fks[key].append(e)

		for pkm in self.pkmulti:
			pkcollst = pkm.db_columns.split(self.colseparator)
			print(pkm)
			# print()

			# for each table in the database, check if we have candidates in fks for this PK, if we do: get cartesian
			# product and store in the fm list
			for table in self.tables:
				tname = table.tablename
				L = []
				for pkcolumn in pkcollst:
					key = (tname, pkcolumn)
					if key not in fks:
						continue
					L.append(fks[key])
				if len(L) == len(pkcollst):
					cart = self.cartesian(L)
					for prod in cart:
						fm.append((prod, pkm))

		for flst,pk in fm:
			pkcollst = pk.db_columns.split(self.colseparator)
			fcols = [ c.columnname for c in flst ]

			fschema = flst[0].db_schema # TBD: ugly indices here
			ftable = flst[0].tablename # TBD: and here

			fsample = self.bottomksketch(self.getDataFn(fschema, fcols, ftable))
			if self.inclusion(fsample, bksketches[(pk.db_schema, pk.tablename, pk.db_columns)]) >= theta:
				quantiles[(pk.db_schema, pk.tablename, pk.db_columns)] = self.quantilehistogram(self.getDataFn(pk.db_schema, pk.db_columns.split(self.colseparator), pk.tablename))
				quantiles[(fschema, ftable, "|".join(fcols))] = self.quantilehistogram(self.getDataFn(fschema, fcols, ftable))
			else:
				fm.remove((flst,pk))

		for flst,pk in fs:
			# only index zero because every fs has only one candidate column...
			quantiles[(flst[0].db_schema, flst[0].tablename, flst[0].columnname)] = self.quantilehistogram(self.getDataFn(flst[0].db_schema, flst[0].columnname, flst[0].tablename))

		result = []
		fall = fs
		fall.extend(fm)

		for f,pk in fall:

			fcols = []
			for cdict in f:
				fcols.append(cdict.columnname)
			fschema = f[0].db_schema # TBD: ugly indices here
			ftable = f[0].tablename # TBD: and here

			if quantiles[(fschema, ftable, "|".join(fcols))] is not None and quantiles[(pk.db_schema, pk.tablename, pk.db_columns)] is not None: # empty columns....
				qfk = quantiles[(fschema, ftable, "|".join(fcols))]
				qpk = quantiles[(pk.db_schema, pk.tablename, pk.db_columns)]

				emdscore = 0
				try:
					for i in range(len(qfk)):
						fkhist = qfk[i][0]
						pkhist = qpk[i][0]

						fkbins = qfk[i][1]
						pkbins = qpk[i][1]

						emdscore += emd.emd(fkhist, pkhist, fkbins[0:-1], pkbins[0:-1])
					emdscore = emdscore/len(qfk[0])
				except:
					emdscore = -1

				if math.isnan(emdscore):
					emdscore = -1

				nfk = ForeignKey(db_catalog=pk.db_catalog, pkdb_schema=pk.db_schema, fkdb_schema=fschema, pktablename=pk.tablename, fktablename=ftable, fk_columns=fcols, keyname='implicit_fk', type='implicit')
				nfk.pk_columns=pk.db_columns
				nfk.score = emdscore

				result.append((nfk, emdscore))

		# print("## len(Q): " + str(len(q)))

		return sorted(result, key=lambda kvp: kvp[1], reverse=False)

def pruneDuplicateFks(fks):
	seen = {}
	pruned = []
	for fk in fks:
		key = (fk.db_catalog, fk.pkdb_schema, fk.fkdb_schema, fk.pktablename, fk.fktablename, fk.pk_columns, fk.fk_columns)
		if key not in seen:
			seen[key] = True
			pruned.append(fk)
	return pruned

def main(args):
	colseparator = '|'
	miner = MetaModel(args.src)

	tables = miner.tables()
	columns = miner.columns()

	dic = {}
	for pk in [ c for c in columns if c.primary_key ]:
		if (pk.info['schemaname'], pk.table.name) not in dic:
			dic[(pk.info['schemaname'], pk.table.name)] = []
		dic[(pk.info['schemaname'], pk.table.name)].append(pk)

	# split primary keys into singlecolumn and multicolumn keys
	spks = [ dic[key] for key in dic.keys() if len(dic[key]) == 1 ]
	mpks = [ dic[key] for key in dic.keys() if len(dic[key]) > 1 ]

	print("LOT'S OF WORK IN PROGRESS...")
	exit()

	affaires = Discovery(tables=tables, columns=columns, pksingle=spks, pkmulti=mkps)

	fks = pruneDuplicateFks([ fk[0] for fk in affaires.discoverfks(0.9) ])

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
	parser.add_argument("-t", "--target", help="connection_string for the target-database", metavar="string")
	parser.add_argument("-d", "--dry_run", help="flag to make a dry-run without storing the result to target database", action='store_true', default=False)
	args = parser.parse_args()

	main(args)
