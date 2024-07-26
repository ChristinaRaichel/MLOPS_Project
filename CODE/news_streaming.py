import http.client, urllib
import os
from dotenv import load_dotenv

load_dotenv()

def stream_function():
    news_api_conn = http.client.HTTPSConnection('api.thenewsapi.com')
    params = urllib.parse.urlencode({
        'api_token': os.getenv('NEWS_API_TOKEN'),
        'limit': 3,
        'language': 'en'
        })

    news_api_conn.request('GET', '/v1/news/all?{}'.format(params))
    response = news_api_conn.getresponse()
    news_data = response.read()
    return news_data.decode('utf-8')