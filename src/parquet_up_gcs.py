'''
What it should do?

- should take data from a folder data/raw
- upload in google cloud in the .parquet format
'''

import os
import pathlib

from credentials.keys.lg_cloud import get_gclient
from l_bucket import GStorage

from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import pandas_gbq



working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/raw')
treated_files_folder = working_dir.joinpath('data/treated')


#Gettig files and transforming them into Dataframe
file_path_list = []

for file in files_folder.iterdir():
    if file.is_file():
        file_path_list.append(file)

dataframes = [pd.read_csv(files) for files in file_path_list]
df = pd.concat(dataframes, ignore_index=True)

#Transforming, saving and uploading parquet files

#transforming and saving
df_parquet = df.to_parquet(
    treated_files_folder.joinpath('full_table.parquet'), 
    compression=None)

#sending to gcloud
STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')
storage_client = get_gclient()
gcs = GStorage(storage_client)
bucket_name = 'demo_bucket_ht'

if not bucket_name in gcs.list_buckets():
    bucket_gcs = gcs.create_bucket('demo_bucket_ht', STORAGE_CLASSES[0])
else:
    bucket_gcs = gcs.get_bucket(bucket_name)

for file_path in treated_files_folder.glob('*.*'):
    gcs.upload_file(bucket_gcs, file_path.name, str(file_path))