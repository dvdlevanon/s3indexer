#!/usr/bin/env python

import argparse
from s3indexer.db import DB
from s3indexer.loader import Loader

parser = argparse.ArgumentParser(description='Index metadata of files in S3.')
parser.add_argument('--bucket_name', '-b', help='S3 bucket name')
parser.add_argument('--table_name', '-t', help='Files table name')
parser.add_argument('--db_conn', '-c', help='Postgres DB Connection string')
args = vars(parser.parse_args())

if not args['bucket_name']:
	print('No bucket name specified')
	exit(1)

if args['db_conn']:
	connection_string = args['db_conn']
else:
	connection_string = "host={} dbname={}".format("localhost", "s3_metadata")

if args['table_name']:
	table_name = args['table_name']
else:
	table_name = "files"

db = DB(connection_string, table_name)
db.init_schema()

loader = Loader(db, args['bucket_name'], 1000)
loader.load()
