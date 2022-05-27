import time
import math
import re

class Analyzer:
	def __init__(self, db, batch_size):
		self.db = db
		self.offset = self.load_offset()
		self.batch_size = batch_size
		self.a_year_ago = time.time() - (60 * 60 * 24 * 30 * 12)
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
			rows_count = self.analyze_batch()
			self.offset = self.offset + rows_count
			self.persist_offset(self.offset)
			if rows_count == 0:
				break
		
	def analyze_batch(self):
		query = 'SELECT k, size, modified, name, storage_class FROM {} OFFSET %s LIMIT %s'.format(self.db.files_table_name)
		result = self.db.execute_fetch_all_with_params(query,
			(self.offset, self.batch_size))
		
		for cur in result:
			self.analyze_row(cur)
		
		return len(result)
		
	def analyze_row(self, cur):
		k = cur[0]
		size = cur[1]
		modified_epoch = cur[2]
		name = cur[3]
		storage_class = cur[4]
		modified = time.gmtime(modified_epoch)
		year = modified.tm_year
		month = modified.tm_mon
		category = self.infer_file_category(name)
		
		if not category:
			print ("Unknown category for file {}".format(name))
			exit(1)
		self.update_by_year(year, size)
		self.update_by_storage_class(storage_class, size)
		self.update_by_mega(math.floor(size / (1024 * 1024)), size)
		if size < 1024 * 1024:
			self.update_by_kilo(math.floor(size / 1024), size)
		if modified_epoch > self.a_year_ago:
			self.update_by_month(month, size)
		self.update_by_category(category, size)
		self.update_by_year_category(year, category, size)
		
	def infer_file_category(self, name):
		for category in self.categories:
			if category['regex'].match(name):
				return category['name']
		return None
		
	def update_by_year(self, year, size):
		self.db.execute_with_params("""
			INSERT INTO by_year AS y (year, files, size) 
			VALUES (%s, %s, %s)
			ON CONFLICT (year) DO UPDATE SET 
				files = y.files + EXCLUDED.files,
				size = y.size + EXCLUDED.size
		""", (year, 1, size))
	
	def update_by_storage_class(self, storage_class, size):
		self.db.execute_with_params("""
			INSERT INTO by_storage_class AS s (storage_class, files, size) 
			VALUES (%s, %s, %s)
			ON CONFLICT (storage_class) DO UPDATE SET 
				files = s.files + EXCLUDED.files,
				size = s.size + EXCLUDED.size
		""", (storage_class, 1, size))
	
	def update_by_mega(self, mega, size):
		self.db.execute_with_params("""
			INSERT INTO by_mega AS m (mega, files, size) 
			VALUES (%s, %s, %s)
			ON CONFLICT (mega) DO UPDATE SET 
				files = m.files + EXCLUDED.files,
				size = m.size + EXCLUDED.size
		""", (mega, 1, size))
	
	def update_by_kilo(self, kilo, size):
		self.db.execute_with_params("""
			INSERT INTO by_kilo AS k (kilo, files, size) 
			VALUES (%s, %s, %s)
			ON CONFLICT (kilo) DO UPDATE SET 
				files = k.files + EXCLUDED.files,
				size = k.size + EXCLUDED.size
		""", (kilo, 1, size))
	
	def update_by_month(self, month, size):
		self.db.execute_with_params("""
			INSERT INTO by_month AS m (month, files, size) 
			VALUES (%s, %s, %s)
			ON CONFLICT (month) DO UPDATE SET 
				files = m.files + EXCLUDED.files,
				size = m.size + EXCLUDED.size
		""", (month, 1, size))
		
	def update_by_category(self, category, size):
		self.db.execute_with_params("""
			INSERT INTO by_category AS c (category, files, size) 
			VALUES (%s, %s, %s)
			ON CONFLICT (category) DO UPDATE SET 
				files = c.files + EXCLUDED.files,
				size = c.size + EXCLUDED.size
		""", (category, 1, size))
	
	def update_by_year_category(self, year, category, size):
		self.db.execute_with_params("""
			INSERT INTO by_year_and_category AS c (year, category, files, size) 
			VALUES (%s, %s, %s, %s)
			ON CONFLICT (year, category) DO UPDATE SET 
				files = c.files + EXCLUDED.files,
				size = c.size + EXCLUDED.size
		""", (year, category, 1, size))
	