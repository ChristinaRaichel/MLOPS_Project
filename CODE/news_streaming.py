import http.client, urllib
import os
from dotenv import load_dotenv

load_dotenv()


conn = http.client.HTTPSConnection('api.thenewsapi.com')

params = urllib.parse.urlencode({
    'api_token': os.getenv('API_TOKEN'),
    'limit': 5,
    'language': 'en'
    })

conn.request('GET', '/v1/news/all?{}'.format(params))

res = conn.getresponse()
data = res.read()

print(data.decode('utf-8'))