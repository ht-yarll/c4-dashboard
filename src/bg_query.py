import io

from credentials.keys.lg_cloud import get_gclient
from credentials.keys.lg_cloud import get_bqclient
from GCStorage import GStorage
from GBQuery import GBigQuery

import pandas as pd

storage_client = get_gclient()
gbq_client = get_bqclient()
gcs = GStorage(storage_client)
gbq = GBigQuery(gbq_client)

# Fetching file from GCS
gcs_blobs = gcs.list_blobs('demo_bucket_ht')
for blob in gcs_blobs:
    data = blob.download_as_bytes()

# Load File in df
df_blob = pd.read_parquet(io.BytesIO(data))

#data treatment
df_blob.columns = (
    df_blob.columns
    .str.strip()
    .str.replace(r'[^a-zA-Z0-9_]', '_', regex=True) #Replace Invalid Character for '_'
    .str.replace(r'[\/\(\)\$\s]', '_', regex=True)
    .str.lower()
)
df_blob = df_blob.replace(r'^\s*$', 'N/A', regex=True) #treat empty data
df_blob = df_blob.loc[:, ~df_blob.T.duplicated()] #exclude duplicates
df_blob = df_blob.reset_index(drop=True)

if 'country' in df_blob.columns:
    df_blob = df_blob.drop(columns=['country'])

# upload to gbq
destinatiion_table = 'blackstone-446301.dataset_a.tabela_hdi'
project_id = 'blackstone-446301'

gbq.up_to_bigquery(df_blob, destinatiion_table, project_id)