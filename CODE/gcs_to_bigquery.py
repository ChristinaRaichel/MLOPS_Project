from google.cloud import bigquery
from google.cloud import storage
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv

load_dotenv()
credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')


spark = SparkSession.builder \
        .appName("GCS to BigQuery ETL") \
        .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
        .config('google.cloud.auth.service.account.enable', 'true') \
        .config('google.cloud.auth.service.account.json.keyfile', credentials) \
        .getOrCreate()



# Set up BigQuery clien
bigquery_client = bigquery.Client()
dataset_id = os.getenv('DATASET_ID')
table_id = os.getenv('TABLE_ID')

# GCS client
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FILE_PREFIX =  os.getenv('GCS_FILE_PREFIX')

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# Load files from GCS to BigQuery
blobs = bucket.list_blobs(prefix=GCS_FILE_PREFIX)
#json_files = [f"gs://{BUCKET_NAME}/{blob.name}" for blob in blobs if blob.name.endswith('.json')]
json_files = 'gs://news_api_stream_bucket/news-articles/2024-07-25T23:35:03.000000Z_de8333a6-dac7-45aa-a035-c32df5e6af81.json'
print(json_files)
df = spark.read.json(json_files)
df = df.select(col("title"), col("categories"), col("snippet"),col("description"),col("language") )

df.write \
.format("bigquery") \
.option("table", f"{dataset_id}.{table_id}") \
.mode("append") \
.save()
        
spark.stop()
