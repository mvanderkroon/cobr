import sys, argparse, json, datetime
sys.path.append("../common")

from MetaModel import MetaModel
from objects import ForeignKey, PrimaryKey, Table, Column, Base

from elasticsearch import Elasticsearch, helpers

from multiprocessing import Pool

from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy import exc as sa_exc

excluded = ['image']

def executeOne(task=None):
	column = task[0]

	esIp = task[1]
	esPort = task[2]
	connection_string = task[3]
	idx = task[4]
	typ = task[5]

	try:
		if column.type not in excluded:
			es = Elasticsearch(host=str(esIp), port=str(esPort), timeout=240)

			actions = []

			engine = create_engine(connection_string)
			conn = engine.connect()

			s = select([column])
			result = conn.execute(s)

			while True:
			    row = result.fetchone()
			    if row is None:
			        break

			    actions.append({
					"_index": idx,
					"_type": typ,
					"value": str(row[0]),
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
		print('')
	finally:
		conn.close()

def main(args):
	esIp = args.ip
	esPort = args.port
	connection_string = args.src
	idx = args.idx
	typ = args.type

	sts = datetime.datetime.now()

	pool = Pool(processes=int(args.cpu))

	# get columns from the meta database and populate the worker tasks
	miner = MetaModel(args.src)
	tasks = [ (c, esIp, esPort, connection_string, idx, typ) for c in miner.columns() ]

	print('## going to populate Elastic Search with {0} columns'.format(len(tasks)-1))
	print('\n'*1)

	# execute on multiple threads
	for i, _ in enumerate(pool.imap_unordered(executeOne, tasks)):
		sys.stdout.write("\033[1A")

		totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(tasks)-1, round(i/(len(tasks)-1)*100,2))
		sys.stdout.write(totalprogress)
		sys.stdout.flush()

	print('')
	print('## DONE.' )
	print('## time elapsed: {0}'.format(str(datetime.datetime.now() - sts)))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--ip", help="Elasticsearch host ip, defaults to 127.0.0.1", metavar="string", default='127.0.0.1')
	parser.add_argument("-p", "--port", help="Elasticsearch host port, defaults to 9200", metavar="string", default='9200')

	parser.add_argument("-x", "--idx", help="Elasticsearch index name, defaults to 'inbox'", metavar="string", default='inbox')
	parser.add_argument("-t", "--type", help="Elasticsearch type name, defaults to 'new'", metavar="string", default='new')

	parser.add_argument("-s", "--src", help="connection_string for the src-database", metavar="string")
	parser.add_argument("-c", "--cpu", help="number of processes to run within the pool, defaults to 4", metavar="string", default='4')
	args = parser.parse_args()

	# currently we catch and ignore all warnings, which is a bit extreme; probably we should output these warning to a log file
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        main(args)