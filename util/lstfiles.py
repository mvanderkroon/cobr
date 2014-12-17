#!/usr/local/bin/python3
import os
import sys
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-d", "--rootdir", dest="rootdir", help="root directory for which we will recursively display file information", metavar="string")
(options, args) = parser.parse_args()

def main():
	fileList = []
	fileSize = 0
	folderCount = 0
	rootdir = options.rootdir

	for root, subFolders, files in os.walk(rootdir):
		folderCount += len(subFolders)
		for file in files:
			f = os.path.join(root,file)
			fileSize = fileSize + os.path.getsize(f)
			#print(f)
			fileList.append(f)

	for file in fileList:
		print("{0:120} {1:20}B {2:20.2f}KB {3:20.2f}MB {4:20.2f}GB".format(file, os.path.getsize(file), os.path.getsize(file)/1024, os.path.getsize(file)/1024/1024, os.path.getsize(file)/1024/1024/1024))

	print("");
	print("Total Size is {0} bytes".format(fileSize))
	print("Total Files ", len(fileList))
	print("Total Folders ", folderCount)

if __name__ == '__main__':
	main()