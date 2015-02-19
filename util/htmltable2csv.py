import os
import requests

from optparse import OptionParser
from bs4 import BeautifulSoup

parser = OptionParser()
parser.add_option("-u", "--url", dest="url", help="url of the HTML page that contains the table. If not prefixed with 'http', local filesystem will be assumed", metavar="string")
parser.add_option("-s", "--selector", dest="selector", help="CSS selector of the table within the HTML page", metavar="string")
parser.add_option("-i", "--index", dest="index", help="index of the object within the result of the CSS selector, defaults to <0>; may be a range of <min,max> in which case the data will be concatenated", metavar="integer", default='0')
parser.add_option("-d", "--delimiter", dest="delimiter", help="output delimiter, defaults to <,>", metavar="string", default=",")
parser.add_option("-q", "--quote", dest="quote", help="quote character, defaults to empty", metavar="string", default="")
(options, args) = parser.parse_args()

def processTable(table):

	lines = ''
	for row in table.findAll('tr'):

		cells = row.findAll('th')
		cells.extend(row.findAll('td'))

		for cell in cells:
			value = cell.find(text=True).encode('utf-8')
			lines += options.quote + value + options.quote + options.delimiter
		lines = lines[:-len(options.delimiter)]
		lines += '\n'
	return lines

def main():

	if (options.url.startswith('http')):
		html = requests.get(options.url).text
	else:
		html = open(options.url, 'r').read()

	soup = BeautifulSoup(html)

	result = ''

	if ',' in options.index:
		min = int(options.index.split(',')[0])
		max = int(options.index.split(',')[1])
	else:
		min = int(options.index)
		max = int(options.index) + 1

	for i in range(min, max):
		try:
			table = soup.select(options.selector)[i]
			result += processTable(table)
		except Exception as ex:
			pass

	print(result[:-1])

if __name__ == '__main__':
	main()