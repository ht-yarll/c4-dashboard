import os
import pathlib
import mimetypes

from google.cloud import storage

STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')

class GStorage:
    def __init__(self, storage_client):
        self.client = storage_client

    def create_bucket(self, bucket_name, storage_class, bucket_location='US'):
        bucket = self.client.bucket(bucket_name)
        bucket.storage_class = storage_class
        return self.client.create_bucket(bucket, bucket_location)
    
    def get_bucket(self, bucket_name):
        return self.client.get_bucket(bucket_name)
    
    def list_buckets(self):
        buckets = self.client.list_buckets()
        return [bucket.name for bucket in buckets]

workin_dir = pathlib.Path.cwd()
files_folder = workin_dir.joinpath('data/raw')
downloads_folder = workin_dir.joinpath('data/treated')
bucket_name = 'try_bucket'

storage_client = storage.Client()
gcs = GStorage(storage_client)

if not bucket_name in gcs.list_buckets():
    bucket_gcs = gcs.create_bucket('try_bucket', STORAGE_CLASSES[0])
else:
    bucket_gcs = gcs.get_bucket(bucket_name)