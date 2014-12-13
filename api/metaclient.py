import sys
sys.path.append("../common")

from objects import ForeignKey, PrimaryKey, Table, Column, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import pymssql

class reader():
	def __init__(self, connection_string=''):
		self.engine = create_engine(connection_string)
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()

	def getTables(self, filter=None):
		retval = []
		try:
			if filter is None:
				for instance in self.session.query(Table): 
					retval.append(instance)
			else:
				for instance in self.session.query(Table).filter(filter):
					retval.append(instance)
		except:
			self.session.rollback()
			raise
		return retval

	def getColumns(self, filter=None):
		retval = []
		try:
			if filter is None:
				for instance in self.session.query(Column): 
					retval.append(instance)
			else:
				for instance in self.session.query(Column).filter(filter):
					retval.append(instance)
		except:
			self.session.rollback()
			raise
		return retval

	def getPrimaryKeys(self, filter=None):
		retval = []
		try:
			if filter is None:
				for instance in self.session.query(PrimaryKey): 
					retval.append(instance)
			else:
				for instance in self.session.query(PrimaryKey).filter(filter):
					retval.append(instance)
		except:
			self.session.rollback()
			raise
		return retval

	def getForeignKeys(self, filter=None):
		retval = []
		try:
			if filter is None:
				for instance in self.session.query(ForeignKey): 
					retval.append(instance)
			else:
				for instance in self.session.query(ForeignKey).filter(filter):
					retval.append(instance)
		except:
			self.session.rollback()
			raise
		return retval

	def close(self):
		self.session.close()

class writer():
	def __init__(self, connection_string=''):
		self.engine = create_engine(connection_string)
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()

	def writeTables(self, tables):
		try:
			for table in tables:
				self.session.add(table)
			self.session.commit()
		except:
			self.session.rollback()
			raise

	def writeColumns(self, columns):
		try:
			for column in columns:
				self.session.add(column)
			self.session.commit()
		except:
			self.session.rollback()
			raise

	def writePrimaryKeys(self, pks):
		try:
			for pk in pks:
				self.session.add(pk)
			self.session.commit()
		except:
			self.session.rollback()
			raise

	def writeForeignKeys(self, fks):
		try:
			for fk in fks:
				self.session.add(fk)
			self.session.commit()
		except:
			self.session.rollback()
			raise

	def reset(self):
		Base.metadata.drop_all(self.engine)
		Base.metadata.create_all(self.engine)

	def close(self):
		self.session.close()

def main():
	pass
	
if __name__ == '__main__':
	main()
