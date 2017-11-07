import tweepy
import pandas as pd
import datetime
import os
import apikeys


def get_followers():
    # OAuth process, using the keys and tokens
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # Creation of the actual interface using authentication
    api = tweepy.API(auth)
    follower_count = api.get_user('NHM_Digitise').followers_count
    # Get runtime + date
    today = datetime.datetime.now()
    new_count = [{'date': today.date(), 'time': today.time(), 'followers': follower_count}]
    row = pd.DataFrame(new_count)
    row.index.names = ['index']
    # write to csv
    if not os.path.exists('followers.csv'):
        row.to_csv('followers.csv')
    else:
        df = pd.read_csv('followers.csv', index_col=0)
        df2 = df.append(row, ignore_index=True)
        df2.to_csv('followers.csv')

# Populate consumer keys and access tokens used for OAuth
consumer_key = apikeys.c_key()
consumer_secret = apikeys.c_secret()
access_token = apikeys.a_token()
access_secret = apikeys.a_secret()

get_followers()
