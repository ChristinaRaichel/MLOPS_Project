from kafka import KafkaConsumer
from google.cloud import storage
import json
import os
import kafka_producer
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_TOPIC = kafka_producer.TOPIC_NAME
KAFKA_BOOTSTRAP_SERVER = kafka_producer.KAFKA_BOOTSTRAP_SERVER
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FILE_PREFIX =  os.getenv('GCS_FILE_PREFIX')


consumer_gcs = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

for message in consumer_gcs:
    article = message.value
    file_name = f"{GCS_FILE_PREFIX}{article['publishedAt']}_{article['source']['id']}.json"
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(article), content_type='application/json')
    print(f"Uploaded {file_name} to GCS")



