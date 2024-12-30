import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'src/credentials/keys/blackstone_ht_sk.json'

storage_client = storage.Client()

bucket_name = 'ht_data_cloud'
bucket = storage_client.bucket(bucket_name)
bucket.location = 'US'
storage_client.create_bucket(bucket)
