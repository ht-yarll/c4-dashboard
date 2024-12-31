from credentials.keys.lg_cloud import get_gclient

from google.cloud import bigquery
import pandas as pd

class GBigQuery:
    def __init__(self, storage_client):
        self.client = storage_client

    def up_to_bigquery(self, file, destination_table, project_id, if_exists='replace'):
        job_config = bigquery.LoadJobConfig()
        if isinstance(file, pd.DataFrame):
            data = self.client.load_table_from_dataframe(
                file, destination_table, job_config = job_config
            )
        else:
            with open(file, 'rb') as file_obj:
                data = self.client.load_table_from_file(
                    file_obj, destination_table, job_config = job_config
                )
        data.result()
        return data