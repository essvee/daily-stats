import tweepy
import pandas as pd
import datetime
import os
import apikeys

# Consumer keys and access tokens, used for OAuth
consumer_key = apikeys.c_key()
consumer_secret = apikeys.c_secret()
access_token = apikeys.a_token()
access_secret = apikeys.a_secret()

# OAuth process, using the keys and tokens
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# Creation of the actual interface using authentication
api = tweepy.API(auth)

follower_count = api.get_user('NHM_Digitise').followers_count
run_date = datetime.datetime.now().date()
run_time = datetime.datetime.now().time()
new_count = [{'date': run_date, 'time': run_time, 'followers': follower_count}]
row = pd.DataFrame(new_count)
row.index.names = ['index']

if not os.path.exists('followers.csv'):
    row.to_csv('followers.csv')
else:
    df = pd.read_csv('followers.csv', index_col=0)
    df2 = df.append(row, ignore_index=True)
    df2.to_csv('followers.csv')