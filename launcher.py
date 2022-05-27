#!/usr/bin/env python

import argparse
from s3indexer.db import DB
from s3indexer.loader import Loader
from s3indexer.analyzer import Analyzer
from s3indexer.analyze_cleaner import AnalyzeCleaner

common_parser = argparse.ArgumentParser(add_help=False)
common_parser.add_argument('--table_name', '-t', help='Files table name', default='files')
common_parser.add_argument('--db_conn', '-c', help='Postgres DB Connection string', default='host={} dbname={}'.format('localhost', 's3_metadata'))

main_parser = argparse.ArgumentParser()
subparsers = main_parser.add_subparsers(dest='command')
load_parser = subparsers.add_parser('load', help='Load files metadata into raw db table', parents=[common_parser])
load_parser.add_argument('--bucket_name', '-b', help='S3 bucket name', required=True)
load_parser.add_argument('--page_size', '-p', help='Load from S3 in batches of (max 1000)', default=1000, type=int)
analyze_parser = subparsers.add_parser('analyze', help='Analyze the raw db table', parents=[common_parser])
analyze_parser.add_argument('--batch_size', '-b', help='Load from DB in batches of', default=1000, type=int)
analyze_parser = subparsers.add_parser('clean-analyze', help='Clean all analyzed results', parents=[common_parser])

args = main_parser.parse_args()

db = DB(args.db_conn, args.table_name)
db.init_schema()

if args.command == 'load':
	print ('Loading files from S3 bucket {} to {}'.format(args.bucket_name, args.table_name))
	loader = Loader(db, args.bucket_name, args.page_size)
	loader.load()
elif args.command == 'analyze':
	print ('Analyzing {}'.format(args.table_name))
	analyzer = Analyzer(db, args.batch_size)
	analyzer.analyze()
elif args.command == 'clean-analyze':
	print ('Cleaning analyze results {}'.format(args.table_name))
	cleaner = AnalyzeCleaner(db)
	cleaner.clean()
