from google.cloud import bigquery
from google.cloud import storage
import os
from dotenv import load_dotenv

load_dotenv()


# Set up BigQuery client
bigquery_client = bigquery.Client()
dataset_id = os.getenv('DATASET_ID')
table_id = os.getenv('TABLE_ID')

# GCS client
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FILE_PREFIX =  os.getenv('GCS_FILE_PREFIX')
print(BUCKET_NAME)

storage_client = storage.Client(project= os.getenv('GCS_PROJECT_NAME'))
bucket = storage_client.bucket(BUCKET_NAME)

# Load files from GCS to BigQuery
blobs = bucket.list_blobs(prefix=GCS_FILE_PREFIX)

for blob in blobs:
    if blob.name.endswith('.json'):
        uri = f"gs://{BUCKET_NAME}/{blob.name}"
        print(uri)
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
