import io

from credentials.keys.lg_cloud import get_gclient
from credentials.keys.lg_cloud import get_bgclient
from GCStorage import GStorage
from GBQuery import GBigQuery

import pandas as pd

storage_client = get_gclient()
gbq_client = get_bgclient()
gcs = GStorage(storage_client)
gbq = GBigQuery(gbq_client)

# Fetching file from GCS
gcs_blobs = gcs.list_blobs('demo_bucket_ht')
for blob in gcs_blobs:
    data = blob.download_as_bytes()

# Load File in df
df_blob = pd.read_parquet(io.BytesIO(data))
# df_blob = df_blob.to_csv()

#data treatment
if 'country' in df_blob.columns:
    df_blob = df_blob.drop(columns=['country'])

df_blob.columns = df_blob.columns.str.strip()
df_blob = df_blob.rename(columns={
    'suicides/100k pop':'suicides_100k_pop',
    'HDI for year':'HDI_for_year',
    'gdp_for_year ($)': 'gdp_for_year',
    'gdp_per_capita ($)':'gdp_per_capita',
    })

df_blob = df_blob.replace(r'^\s*$', 'N/A', regex=True)


# upload to gbq
destinatiion_table = 'blackstone-446301.dataset_a.tabela_hdi'
project_id = 'blackstone-446301'

gbq.up_to_bigquery(df_blob, destinatiion_table, project_id)