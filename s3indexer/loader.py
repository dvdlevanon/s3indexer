import os
import sys
import boto3
import os

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
        
        for items in response:
            for item in items['Contents']:
                self.persist_item(item)
            
            if 'NextContinuationToken' in items:
                self.persist_next_token(items['NextContinuationToken'])
            else:
                print('Next token not found on response, its OK if there are no more files')
            
    def persist_next_token(self, next_token):
        print("Persisting next_token {}".format(next_token))
        self.db.execute_with_params("UPDATE next_token SET next_token=%s WHERE k=%s",
            (next_token, 0))
        
    def persist_item(self, item):
        if not 'Key' in item or not 'Size' in item or not 'StorageClass' in item or not 'LastModified' in item:
            print ("Skipping invalid item {}".format(item))
            return
        
        key = item['Key']
        size = item['Size']
        storageClass = item['StorageClass']
        modified = int(item['LastModified'].timestamp())
        
        print("Persisting item {} {} {} {}".format(key, size, storageClass, modified))
        self.db.execute_with_params("""
            INSERT INTO files (k, size, modified, name, storage_class) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (key, size, modified, os.path.basename(key), storageClass))
