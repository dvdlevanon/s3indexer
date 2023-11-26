import time
import math
import sys
import re
from s3indexer.utils import Utils
from concurrent import futures

class Analyzer:
	def __init__(self, db, batch_size):
		self.db = db
		self.offset = self.load_offset()
		self.batch_size = batch_size
		self.a_year_ago = time.time() - (60 * 60 * 24 * 30 * 12)
		self.thread_pool = futures.ThreadPoolExecutor(max_workers=20)
		self.categories = [
			{
				'name': "zip",
				'regex': re.compile(r".*\.zip")
			},
			{
				'name': "csv",
				'regex': re.compile(r".*\.csv")
			},
			{
				'name': "html",
				'regex': re.compile(r".*\.html")
			}
		]
		
	def load_offset(self):
		offset = self.db.execute_with_params_fetch_one('SELECT row_offset FROM analyze_status WHERE table_name = %s', (self.db.files_table_name,))
		return 0 if not offset else offset[0]
	
	def persist_offset(self, offset):
		self.db.execute_with_params("""
			INSERT INTO analyze_status (table_name, row_offset) 
			VALUES (%s, %s) 
			ON CONFLICT (table_name) DO UPDATE 
			SET row_offset = EXCLUDED.row_offset
		""", (self.db.files_table_name, offset))
	
	def analyze(self):
		while True:
			batch_start_time = Utils.current_time_milli()
			rows_count, query_millis, process_millis, flush_millis = self.analyze_batch()
			batch_time = Utils.current_time_milli() - batch_start_time
			self.offset = self.offset + rows_count
			self.persist_offset(self.offset)
			if rows_count == 0:
				break
			print("Analyzed {} rows in {}ms (query {}ms) (process {}ms) (flush {}ms)".format(
				rows_count, batch_time, query_millis, process_millis, flush_millis))
			sys.stdout.flush()
	
	def analyze_batch(self):
		query = 'SELECT k, size, modified, name, storage_class FROM {} OFFSET %s LIMIT %s'.format(self.db.files_table_name)
		query_start_time = Utils.current_time_milli()
		result = self.db.execute_fetch_all_with_params(query,
			(self.offset, self.batch_size))
		query_millis = Utils.current_time_milli() - query_start_time
		
		memory_tables = BatchMemoryTables()
		
		process_start_time = Utils.current_time_milli()
		for cur in result:
			self.analyze_row(cur, memory_tables)
		process_millis = Utils.current_time_milli() - process_start_time
		
		flush_start_time = Utils.current_time_milli()
		if len(result) > 0:
			memory_tables.flush(self.db, self.thread_pool)
		flush_millis = Utils.current_time_milli() - flush_start_time
		
		return len(result), query_millis, process_millis, flush_millis
	
	def analyze_row(self, cur, memory_tables):
		k = cur[0]
		size = cur[1]
		modified_epoch = cur[2]
		name = cur[3]
		storage_class = cur[4]
		modified = time.gmtime(modified_epoch)
		year = modified.tm_year
		month = modified.tm_mon
		category = self.infer_file_category(name)
		mega = math.floor(size / (1024 * 1024))
		kilo = math.floor(size / 1024)
		
		if not category:
			print ("Unknown category for file {}".format(name))
			sys.stdout.flush()
			exit(1)
		
		memory_tables.by_year.add(year, size)
		memory_tables.by_storage_class.add(storage_class, size)
		memory_tables.by_mega.add(mega, size)
		memory_tables.by_category.add(category, size)
		memory_tables.by_year_and_category.add(year, category, size)
		if size < 1024 * 1024:
			memory_tables.by_kilo.add(kilo, size)
		if modified_epoch > self.a_year_ago:
			memory_tables.by_month.add(month, size)
		
	def infer_file_category(self, name):
		for category in self.categories:
			if category['regex'].match(name):
				return category['name']
		return None

class KeyStats:
	def __init__(self):
		self.count = 0
		self.size = 0

class SingleKeyMemoryTable:
	def __init__(self, table_name, key_name):
		self.keys = {}
		self.table_name = table_name
		self.key_name = key_name
		
	def add(self, key, size):
		if not key in self.keys:
			self.keys[key] = KeyStats()
		
		self.keys[key].count = self.keys[key].count + 1
		self.keys[key].size = self.keys[key].size + size
	
	def flush(self, db):
		for key, value in self.keys.items():
			db.execute_with_params("""
				INSERT INTO """ + self.table_name + """ AS t (""" + self.key_name + """, files, size) 
				VALUES (%s, %s, %s)
				ON CONFLICT (""" + self.key_name + """) DO UPDATE SET 
					files = t.files + EXCLUDED.files,
					size = t.size + EXCLUDED.size
			""", (key, value.count, value.size))

class DoubleKeysMemoryTable:
	def __init__(self, table_name, key_name1, key_name2):
		self.keys = {}
		self.table_name = table_name
		self.key_name1 = key_name1
		self.key_name2 = key_name2
		
	def add(self, key1, key2, size):
		key = (key1, key2)
		if not key in self.keys:
			self.keys[key] = KeyStats()
		
		self.keys[key].count = self.keys[key].count + 1
		self.keys[key].size = self.keys[key].size + size
	
	def flush(self, db):
		for key, value in self.keys.items():
			db.execute_with_params("""
				INSERT INTO """ + self.table_name + """ AS c (""" + self.key_name1 + """, """ + self.key_name2 + """, files, size) 
				VALUES (%s, %s, %s, %s)
				ON CONFLICT (""" + self.key_name1 + """, """ + self.key_name2 + """) DO UPDATE SET 
					files = c.files + EXCLUDED.files,
					size = c.size + EXCLUDED.size
			""", (key[0], key[1], value.count, value.size))

class BatchMemoryTables:
	def __init__(self):
		self.by_year = SingleKeyMemoryTable("by_year", "year")
		self.by_storage_class = SingleKeyMemoryTable("by_storage_class", "storage_class")
		self.by_mega = SingleKeyMemoryTable("by_mega", "mega")
		self.by_kilo = SingleKeyMemoryTable("by_kilo", "kilo")
		self.by_month = SingleKeyMemoryTable("by_month", "month")
		self.by_category = SingleKeyMemoryTable("by_category", "category")
		self.by_year_and_category = DoubleKeysMemoryTable("by_year_and_category", "year", "category")
		
	def flush(self, db, thread_pool):
		futures = []
		futures.append(thread_pool.submit(self.by_year.flush, db))
		futures.append(thread_pool.submit(self.by_storage_class.flush, db))
		futures.append(thread_pool.submit(self.by_mega.flush, db))
		futures.append(thread_pool.submit(self.by_kilo.flush, db))
		futures.append(thread_pool.submit(self.by_month.flush, db))
		futures.append(thread_pool.submit(self.by_category.flush, db))
		futures.append(thread_pool.submit(self.by_year_and_category.flush, db))
		
		for future in futures:
			future.result()
