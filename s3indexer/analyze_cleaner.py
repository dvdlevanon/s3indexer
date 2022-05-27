class AnalyzeCleaner:
	def __init__(self, db):
		self.db = db
	
	def clean(self):
		self.db.execute("DROP INDEX uniq_idx_by_year_and_category")
		self.db.execute("DROP TABLE analyze_status")
		self.db.execute("DROP TABLE by_year")
		self.db.execute("DROP TABLE by_storage_class")
		self.db.execute("DROP TABLE by_mega")
		self.db.execute("DROP TABLE by_kilo")
		self.db.execute("DROP TABLE by_month")
		self.db.execute("DROP TABLE by_category")
		self.db.execute("DROP TABLE by_year_and_category")
		