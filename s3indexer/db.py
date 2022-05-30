import psycopg

class DB:
	def __init__(self, connectionstring, table_name):
		self.conn = psycopg.connect(connectionstring)
		self.files_table_name = table_name
	
	def execute(self, sql):
		with self.conn.cursor() as cur:
			cur.execute(sql)
			self.conn.commit()
	
	def execute_with_params(self, sql, params):
		with self.conn.cursor() as cur:
			cur.execute(sql, params)
			self.conn.commit()
	
	def execute_fetch_one(self, sql):
		with self.conn.cursor() as cur:
			cur.execute(sql)
			return cur.fetchone()
	
	def execute_fetch_all_with_params(self, sql, params):
		with self.conn.cursor() as cur:
			cur.execute(sql, params)
			return cur.fetchall()
	
	def execute_with_params_fetch_one(self, sql, params):
		with self.conn.cursor() as cur:
			cur.execute(sql, params)
			return cur.fetchone()
			
	def init_schema(self):
		self.execute("""
				CREATE TABLE IF NOT EXISTS """ + self.files_table_name + """ (
					k text PRIMARY KEY,
					size integer,
					modified integer,
					name text,
					storage_class text
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS next_token (
					k integer PRIMARY KEY,
					next_token text
				)
			""")
		
		self.execute("INSERT INTO next_token VALUES (0, '') ON CONFLICT DO NOTHING")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS analyze_status (
					table_name text PRIMARY KEY,
					row_offset integer
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS by_year (
					year integer PRIMARY KEY,
					files bigint,
					size bigint
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS by_storage_class (
					storage_class text PRIMARY KEY,
					files bigint,
					size bigint
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS by_mega (
					mega integer PRIMARY KEY,
					files bigint,
					size bigint
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS by_kilo (
					kilo integer PRIMARY KEY,
					files bigint,
					size bigint
				)
			""")

		self.execute("""
				CREATE TABLE IF NOT EXISTS by_month (
					month integer PRIMARY KEY,
					files bigint,
					size bigint
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS by_category (
					category text PRIMARY KEY,
					files bigint,
					size bigint
				)
			""")
		
		self.execute("""
				CREATE TABLE IF NOT EXISTS by_year_and_category (
					year integer NOT NULL,
					category text NOT NULL,
					files bigint,
					size bigint
				)
			""")
		
		self.execute("CREATE UNIQUE INDEX IF NOT EXISTS uniq_idx_by_year_and_category ON by_year_and_category(year, category)")
		