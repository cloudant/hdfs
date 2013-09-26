# Exporting JSON documents from Cloudant to Hadoop

This package is a simple external tool to export JSON documents from a [Cloudant](https://docs.cloudant.com) database to an external REST endpoint.  In `cloudant2hdfs.py` we demonstrate exporting to the [WebHDFS](http://hadoop.apache.org/docs/r1.0.4/webhdfs.html) endpoint, a REST layer on the Hadoop Distributed File System.  Every Cloudant document is mapped directly to a single file in HDFS.

## Overview

### Data Source

We use the Cloudant [`_changes` feed](https://docs.cloudant.com/api/database.html#obtaining-a-list-of-changes) to listen for created, updated or deleted documents.  The `_changes` feed is consumed incrementally for efficient exports.  A unique `update_sequence` string is supplied for every row returned from the changes feed (`row['seq']`).  We checkpoint that update_sequence in the local file `.checkpoint`, and we pass that to the next call to the `_changes` feed (e.g. upon running the script a second time) using the `last=value_of_checkpoint_file` query parameter.  

### Data Target

This export requires a valid HDFS install with WebHDFS enabled.  A useful link for stand-alone Hadoop installs for testing can be found [here](http://importantfish.com/how-to-install-hadoop-on-mac-os-x/).  Note that the only authentication required for WebHDFS is a username on the HDFS cluster that has `rex` permissions. 

### Notes

* Documents are stored with the naming convention `<doc._id>.json` where `_id` is the globally unique Cloudant document identifier.
* We do not compress documents before storing.
* We currently ignore document DELETE notifications.
* Document update notifications results in overwriting the existing file on WebHDFS with the new content of the document body.
* We replace `:` characters in the the document `_id` with '_' characters to satisfy the filename requirements for HDFS.
* The checkpoint is recorded every 100 changes that are successfully handled.
* An un-handled exception will trigger recording of the most recent checkpoint and termination of program execution.

## Requirements

* HTTP accessible Cloudant REST source.
* HTTP accessible WebHDFS REST target.
* The following python libraries: [couchdbkit](http://couchdbkit.org), [pywebhdfs](http://pythonhosted.org/pywebhdfs/)
* A username and password for https access to your Cloudant source (see below).
* A username for access to your WebHDFS target (see below).

## Authentication

Authentication detail for Cloudant and WebHDFS are stored in a local file called `.clou`.  It is expected to be located at `$HOME/.clou` in the unix user space, and to have the following structure:

	[cloudant]
	user = <username>
	password = <pwd>
	
	[webhdfs]
	user = <username>


## Execution

### Options

Execution options can be obtained in the usual fashion:

	./cloudant2hdfs.py -h
	
	Options:
	 -h, --help            show this help message and exit
	 -s LAST_SEQ, --sequence=LAST_SEQ
	                       [REQUIRED] Last good udpate sequence to use as
	                       checkpoint
	 -u URI, --uri=URI     [REQUIRED] URI of Cloudant database (e.g.
	                       `mlmiller.cloudant.com`)
	 -d DBNAME, --dbname=DBNAME
	                       [REQUIRED] Name of Cloudant database (e.g.
	                       `database1`)
	 -t HDFS_HOST, --target=HDFS_HOST
	                        HDFS Host (default=`localhost`)
	 -p HDFS_PORT, --port=HDFS_PORT
	                       HDFS Port (default=50070)
	 -l HDFS_PATH, --location=HDFS_PATH
	                       [REQUIRED] HDFS Directory (e.g.
	                       `user/test/fromcloudant`)

### Performing a first export

To perform a first export we can specify a non-valid update_seq as an argument:

	./cloudant2hdfs.py --uri=cs.cloudant.com --dbname=pager_flow --location=mlmiller/cs.cloudant.com/pager_flow2 --sequence=0
	
This will:

* Consume the entire `_changes` feed for the database `https://cs.cloudant.com/pager_flow` 
* POST each document to the default webhdfs `http://localhost:50070` service
* Store each `<id>.json` file in the HDFS directory `mlmiller/cs.cloudant.com/pager_flow2`.
* Create a local `.checkpoint` file that contains the last valid processed update_sequence.

### Perform an incremental export

Upon program termination (e.g. an unhanded exception due to an issue on the source or target) we can safely resume where we left off last:

	./cloudant2hdfs.py --uri=cs.cloudant.com --dbname=pager_flow --location=mlmiller/cs.cloudant.com/pager_flow2 --sequence=`cat .checkpoint`
	
where we have simply piped the content of the `.checkpoint` file into the command line argument for the script.

## Potential extensions

* Using a filter for the consumption of the `_changes` feed.  This simple but powerful extension requires simply adding a runtime option and extending the call to the `ChangesStream`.  Many similar additions are available and document in detail [here](https://docs.cloudant.com/api/database.html#obtaining-a-list-of-changes)
* Compression of large documents.  For large documents, compressing the JSON bodies before sending over the wire may be a worthy optimization.
* Concurrency.  If the target WebHDFS system becomes a rate limit, one can add concurrency to handle multiple `PyWebHdfsClient` clients at once.

