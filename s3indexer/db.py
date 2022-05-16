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
		