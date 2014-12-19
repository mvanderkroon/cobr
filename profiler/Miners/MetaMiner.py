from objects import ForeignKey, PrimaryKey, Table, Column

from MssqlMetaMiner import MssqlMetaMiner
from MysqlMetaMiner import MysqlMetaMiner

class MetaMiner():
	def __init__(self, type='pymssql'):
		self.type = type

		self.classes = {
			'pymssql': MssqlMetaMiner,
			'pymysql': MysqlMetaMiner
		}

	def getInstance(self, db_catalog='', db_host='127.0.0.1', db_user='', db_password=''):	
		return self.classes[self.type](db_catalog=db_catalog, db_host=db_host, db_user=db_user, db_password=db_password)

