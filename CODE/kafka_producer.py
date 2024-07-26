import confluent_kafka
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import news_streaming
import time
import json


load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_API_KEY = os.getenv('KAFKA_API_KEY')
KAFKA_API_SECRET = os.getenv('KAFKA_API_SECRET')
KAFKA_REST_API = os.getenv('KAFKA_REST_API')


conf = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVER,
    'security_protocol': 'SASL_SSL',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': KAFKA_API_KEY,
    'sasl_plain_password': KAFKA_API_SECRET,
    'value_serializer': lambda v: json.dumps(v).encode('utf-8') if v is not None else None

}

#producer = confluent_kafka.Producer(**conf)
producer = KafkaProducer(**conf)

TOPIC_NAME = os.getenv('TOPIC_NAME')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

while True:
    news_data = news_streaming.stream_function()
    news_data = json.loads(news_data)

    for news in news_data['data']:
        producer.send(TOPIC_NAME, news)
    print('1 poll')

    # Sleep for a specified time before fetching again
    time.sleep(120)
#producer.close()
