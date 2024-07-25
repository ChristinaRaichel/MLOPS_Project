import tweepy
import os

API_KEY = os.getenv(TWITTER_API_KEY)
API_SECRET_KEY = os.getenv(TWITTER_API_SECRET)
ACCESS_TOKEN = os.getenv(ACCESS_TOKEN)
ACCESS_TOKEN_SECRET = os.getenv(ACCESS_SECRET)

# Authenticate to Twitter
auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# Create API object
api = tweepy.API(auth, wait_on_rate_limit=True)

WOEID = 1  # Global trends. Change to a specific location WOEID if needed
trending_topics = api.trends_place(WOEID)

# Extract top 10 trending topics
top_trends = [trend['name'] for trend in trending_topics[0]['trends'][:10]]
print("Top 10 trending topics:", top_trends)

for trend in top_trends:
    print(f"Fetching tweets for trend: {trend}")
    tweets = api.search_tweets(q=trend, count=2, result_type='popular', lang='en')
    for tweet in tweets:
        print(f"User: {tweet.user.screen_name}")
        print(f"Tweet: {tweet.text}")
        print("----")
