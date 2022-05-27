import os
import sys
import boto3
import time

class Loader:
    def __init__(self, db, bucket_name, page_size):
        old_next_token = db.execute_fetch_one("SELECT next_token FROM next_token WHERE k = 0")

        self.next_token = old_next_token[0] if old_next_token[0] != '' else None
        self.bucket_name = bucket_name
        self.page_size = page_size
        self.session = boto3.session.Session()
        self.s3 = self.session.client('s3')
        self.paginator = self.s3.get_paginator('list_objects_v2')
        self.db = db
    
    def load(self):
        config = {'PageSize':self.page_size, 'StartingToken':self.next_token}
        response = self.paginator.paginate(Bucket=self.bucket_name, PaginationConfig=config)
        
        last_printed_time = Utils.current_time_milli()
        last_printed = 0
        loaded_count = 0
        db_time = 0
        string_concat_time = 0
        
        for items in response:
            string_concat_start = Utils.current_time_milli()
            sql_statements = ''
            for item in items['Contents']:
                if not self.is_valid_item(item):
                    print ("Invalid item {}".format(item))
                    continue
                sql_statements += self.get_item_sql(self.get_item_sql_values(item), self.db.files_table_name)
                loaded_count = loaded_count + 1
            string_concat_time = string_concat_time + (Utils.current_time_milli() - string_concat_start)
            
            # According to https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
            #  batch performance is better achieved with plain old (and ugly) string concat
            # 
            # Open to sql-injection if s3 file names contain something fishy
            # 
            db_start = Utils.current_time_milli()
            self.db.execute(sql_statements)
            db_time = db_time + (Utils.current_time_milli() - db_start)
            
            if loaded_count - last_printed > 100000:
                current_time = Utils.current_time_milli()
                print("{} files loaded to db in {}ms (db_time: {}) (string_concat_time: {})".format(
                    loaded_count, current_time - last_printed_time, db_time, string_concat_time))
                sys.stdout.flush()
                last_printed = loaded_count
                last_printed_time = current_time
                db_time = 0
                string_concat_time = 0
            
            if 'NextContinuationToken' in items:
                self.persist_next_token(items['NextContinuationToken'])
            else:
                print('Next token not found on response, its OK if there are no more files')
            
    def persist_next_token(self, next_token):
        self.db.execute_with_params("UPDATE next_token SET next_token=%s WHERE k=%s",
            (next_token, 0))
    
    def is_valid_item(self, item):
        return 'Key' in item and 'Size' in item and 'StorageClass' in item and 'LastModified' in item
        
    def get_item_sql_values(self, item):
        key = item['Key']
        size = item['Size']
        storageClass = item['StorageClass']
        modified = int(item['LastModified'].timestamp())
        
        return "'{}', {}, {}, '{}', '{}'".format(key, size, modified, os.path.basename(key), storageClass)

    def get_item_sql(self, values, files_table_name):
        return "INSERT INTO " + files_table_name + " (k, size, modified, name, storage_class) VALUES (" + values + ") ON CONFLICT DO NOTHING;"
