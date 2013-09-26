#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, json, traceback, re
from optparse import OptionParser
from couchdbkit import Server, Database, ChangesStream
from pywebhdfs.webhdfs import PyWebHdfsClient
from ConfigParser import ConfigParser

#we make ample use of these docs.
#pywebdfs: http://pythonhosted.org/pywebhdfs/
#couchdbkit: http://couchdbkit.org

#todo:
# test at scale
# add readme

def get_creds(f):
	""" Retrieve necessary credentials from file.

	Credential files follow the ConfigParse guidelines and are structured as:
	   [Cloudant]
	   user = <username>
	   password = <password>

	   [webhdfs]
	   user = <user with `rwx` rights in hdfs
	"""

	config = ConfigParser()
	config.read(f)
	creds = {}
	creds['cloudant_user'] = config.get('cloudant','user')
	creds['cloudant_pwd'] = config.get('cloudant','password')
	creds['hdfs_user'] = config.get('webhdfs','user')
	return creds

def processDoc(hdfs, doc, hdfs_path):
	""" POST a single document to webhdfs.

	By default an updated document will overwrite the state of an existing file.
	"""
	#cloudant allows `:` in a document id, but hdfs doesn't
	#we swap those out
	docid = doc['_id'].replace(':','_')

	fname = '%s/%s.json' % (hdfs_path, docid)
	print '\twriting _id:\t%s\twith _rev:\t%s\tas:\t%s' % (doc['_id'],doc['_rev'], fname)
	hdfs.create_file(fname, json.dumps(doc), overwrite=True)

def processChange(hdfs, change, hdfs_path):
	""" Process a single line of the Cloudant `_changes` feed.

	This method assumes it is passed a feed generated with `include_docs=true`.
	We take no action for documents that are deleted.
	"""

	if change.has_key('deleted') or change.has_key('doc')==False:
		return
	processDoc(hdfs, change['doc'], hdfs_path)
	return change['seq']

def checkpoint(seq):
	""" Write last known checkpoint to a local file.

	The `_changes` feed can be consumed incrementally by 
	  (a) recording the value of the `seq` value returend with each row and 
	  (b) passing that with the `?since=<last_good_seq>` argument on next call

	This method writes the `seq` value for the last row that was processed to 
	a file called `.checkpoint`
	"""
	print 'record checkpoint:\t', seq
	f = open('.checkpoint','w')
	f.write(seq)
	f.close()

def checkRequiredArguments(opts, parser):
	""" Validate Required Arguments
	"""
	missing_options = []
	for option in parser.option_list:
		if re.match(r'^\[REQUIRED\]', option.help) and eval('opts.' + option.dest) == None:
			missing_options.extend(option._long_opts)
	if len(missing_options) > 0:
		parser.error('Missing REQUIRED parameters: ' + str(missing_options))

def configureOptions():
	""" Configure the run-time options for the program.

	To see all options, requirements and defaults execute:

	    ./cloudant2hdfs.py -h

	"""
	parser = OptionParser()
	parser.add_option("-s", "--sequence", action="store", dest="last_seq",
		help="[REQUIRED] Last good udpate sequence to use as checkpoint", 
		metavar="LAST_SEQ")
	parser.add_option("-u", "--uri", action="store", dest="uri",
		help="[REQUIRED] URI of Cloudant database (e.g. `mlmiller.cloudant.com`)", 
		metavar="URI")
	parser.add_option("-d", "--dbname", action="store", dest="dbname",
		help="[REQUIRED] Name of Cloudant database (e.g. `database1`)", 
		metavar="DBNAME")
	parser.add_option("-t", "--target", action="store", dest="hdfs_host", 
		default='localhost', help=" HDFS Host (default=`localhost`)", 
		metavar="HDFS_HOST")
	parser.add_option("-p", "--port", action="store", dest="hdfs_port", 
		default='50070', help="HDFS Port (default=50070)", 
		metavar="HDFS_PORT")
	parser.add_option("-l", "--location", action="store", dest="hdfs_path",
		help="[REQUIRED] HDFS Directory (e.g. `user/test/fromcloudant`)", 
		metavar="HDFS_PATH")
	return parser


def main(argv):
	""" Main method.

	This method performs the following tasks:
	1. Parse command line arguments
	2. Retrieve credentials and connect to Cloudant and WebHDFS
	3. Connect to the Cloudant `_changes` feed for checkpointed document consumption
	4. Process each change individually.
	5. Upon exception throwing, store the latest checkpoint to local file and exit.
	"""

	#add options into the parser
	parser = configureOptions()
	(options, args) = parser.parse_args()
	checkRequiredArguments(options, parser)
	print options

	# configurations
	last_seq = options.last_seq

	#get credential
	perm_file = '%s/.clou' % os.environ['HOME']
	creds = get_creds(perm_file)

	#connect to source database
	s = Server('https://%s:%s@%s' % (creds['cloudant_user'], creds['cloudant_pwd'], 
		options.uri))
	db = s[options.dbname]
	#print db.info()

	#connect to target hdfs cluster
	hdfs = PyWebHdfsClient(host=options.hdfs_host,port=options.hdfs_port, 
		user_name=creds['hdfs_user'])
	hdfs.make_dir(options.hdfs_path)

	#and here we consume the cloudant `_changes` feed
	counter = 0
	for c in ChangesStream(db, include_docs=True, heartbeat=True, since=last_seq):
		#print c
		try:
			if counter%100 == 0:
				checkpoint(last_seq)
			seq = processChange(hdfs, c, options.hdfs_path)
			if seq: #protect against the last line being blank
				last_seq = seq
				counter += 1
		except Exception as ex:
			traceback.print_exc()
			checkpoint(last_seq)
			os._exit(1)

	checkpoint(last_seq)


if __name__ == '__main__':
  main(sys.argv)
