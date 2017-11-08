import tweepy
import pandas as pd
import datetime
import os


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

# Get keys
f = open('apikeys.txt', 'r')
keys = f.read().splitlines()
consumer_key = keys[0]
consumer_secret = keys[1]
access_token = keys[2]
access_secret = keys[3]

get_followers()
