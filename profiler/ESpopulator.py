import sys, argparse, json, datetime
sys.path.append("../common")

from MetaModel import MetaModel
from objects import ForeignKey, PrimaryKey, Table, Column, Base

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from multiprocessing import Pool

from sqlalchemy import create_engine
from sqlalchemy.sql import select

excluded = ['image']

def executeOne(unit=None):
	column = unit[0]
	esIp = unit[1]
	esPort = unit[2]
	connection_string = unit[3]

	try:
		if column.type not in excluded:
			es = Elasticsearch(host=str(esIp), port=str(esPort), timeout=240)

			actions = []

			engine = create_engine(connection_string)
			conn = engine.connect()

			s = select([column])
			result = conn.execute(s)

			values = [d[0] for d in result.fetchall()]

			conn.close()

			for j,value in enumerate(values):
				actions.append({
					"_index": "projects",
					"_type": "fb",
					"value": str(value),
					"tablename": column.table.name,
					"columnname": column.name,
					"db_catalog": column.info['db_catalog'],
					"db_schema": column.info['schemaname'],
					"datatype": str(column.type)
				})

			helpers.bulk(es, actions)
	except Exception as e:
		print("could not index column: " + str(column.name))
		print(e)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--ip", help="Elasticsearch host ip, defaults to 127.0.0.1", metavar="string", default='127.0.0.1')
	parser.add_argument("-p", "--port", help="Elasticsearch host port, defaults to 9200", metavar="string", default='9200')
	parser.add_argument("-s", "--src", help="connection_string for the src-database", metavar="string")
	args = parser.parse_args()

	esIp = args.ip
	esPort = args.port
	connection_string = args.src

	sts = datetime.datetime.now()

	pool = Pool(processes=32)

	# get columns from the meta database
	miner = MetaModel(args.src)
	units = [ (c, esIp, esPort, connection_string) for c in miner.columns() ]

	print('## going to populate Elastic Search with {0} columns'.format(len(units)-1))
	print('\n'*1)

	# execute on multiple threads
	for i, _ in enumerate(pool.imap_unordered(executeOne, units)):
		sys.stdout.write("\033[1A")

		totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(units)-1, round(i/(len(units)-1)*100,2))
		sys.stdout.write(totalprogress)
		sys.stdout.flush()

	print('')
	print('## DONE.' )
	print('## time elapsed: {0}'.format(str(datetime.datetime.now() - sts)))

if __name__ == '__main__':
	main()