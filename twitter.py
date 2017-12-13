import tweepy
import datetime
import pymysql


def get_followers():
    # OAuth process, using the keys and tokens
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # Creation of the actual interface using authentication
    api = tweepy.API(auth)
    follower_count = api.get_user('NHM_Digitise').followers_count
    # Get runtime + date
    today_dt = datetime.datetime.today().date()
    # Connect to database
    db = pymysql.connect(host='localhost', user='root', password='r3ptar', db='dashboard')
    cursor = db.cursor()

    # Write update to twitter_followers
    try:
        # Get most recent follower count
        cursor.execute("SELECT follower_count FROM twitter_followers "
                       "WHERE DATE IN (SELECT MAX(date) FROM twitter_followers);")
        result = cursor.fetchone()
        # Calculate change in followers since last period
        lastcount = follower_count - result[0]
        # Add new row and commit
        sql = "INSERT INTO twitter_followers(id, date, follower_count, new_followers) " \
              "VALUES(null, '%s', %s, %s);" % (today_dt, follower_count, lastcount)
        cursor.execute(sql)
        db.commit()
    except pymysql.Error:
        db.rollback()

    cursor.close()
    db.close()

# Get keys
f = open('apikeys.txt', 'r')
keys = f.read().splitlines()
consumer_key = keys[0]
consumer_secret = keys[1]
access_token = keys[2]
access_secret = keys[3]

get_followers()
