from kafka import KafkaConsumer
from google.cloud import storage
import json
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# Configuration
KAFKA_TOPIC = os.getenv('TOPIC_NAME')
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_API_KEY = os.getenv('KAFKA_API_KEY')
KAFKA_API_SECRET = os.getenv('KAFKA_API_SECRET')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FILE_PREFIX =  os.getenv('GCS_FILE_PREFIX')


consumer_gcs = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    security_protocol = 'SASL_SSL',
    sasl_plain_username = KAFKA_API_KEY,
    sasl_plain_password = KAFKA_API_SECRET,
    sasl_mechanism = 'PLAIN',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


storage_client = storage.Client(project= os.getenv('GCS_PROJECT_NAME'))
bucket = storage_client.bucket(GCS_BUCKET_NAME)

def upload_to_gcs(article):
    try:
        file_name = f"{GCS_FILE_PREFIX}{article['published_at']}_{article['uuid']}.json"
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(article), content_type='application/json')
        logging.info(f"Uploaded to GCS")
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {e}")

while consumer_gcs:
    for message in consumer_gcs:
        print(message.value)
        upload_to_gcs(message.value)
    
