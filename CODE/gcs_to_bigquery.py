from google.cloud import bigquery
from google.cloud import storage
import os

# Set up BigQuery client
bigquery_client = bigquery.Client()
dataset_id = 'news_dataset'
table_id = ' news_data_table'

GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FILE_PREFIX =  os.getenv('GCS_FILE_PREFIX')

# GCS client
storage_client = storage.Client()   
bucket = storage_client.bucket(GCS_BUCKET_NAME)

# Load files from GCS to BigQuery
blobs = bucket.list_blobs(prefix=GCS_FILE_PREFIX)
for blob in blobs:
    if blob.name.endswith('.json'):
        uri = f"gs://{GCS_BUCKET_NAME}/{blob.name}"
        load_job = bigquery_client.load_table_from_uri(
            uri,
            f"{dataset_id}.{table_id}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            ),
        )
        load_job.result()  # Waits for the job to complete
        print(f"Loaded {uri} to {dataset_id}.{table_id}")
