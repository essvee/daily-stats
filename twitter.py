import tweepy
import datetime
import gbif_dbtools as db


def get_followers():
    # get keys
    consumer_key, consumer_secret, access_token, access_secret = db.get_keys('apikeys.txt')

    # OAuth process, using the keys and tokens
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    # Get today's follower count for @NHM_Digitise
    follower_count = api.get_user('NHM_Digitise').followers_count

    # Get runtime + date
    today_dt = datetime.datetime.today().date()

    # Connect to database and get last recorded follower count
    sql_query = "SELECT follower_count FROM twitter_followers WHERE DATE IN (SELECT MAX(date) FROM twitter_followers);"
    previous_follower_count = db.query_db(sql_query).fetchone()

    # Calculate change in followers since last run
    last_count = follower_count - previous_follower_count[0]

    # Create update query, add new row and commit
    sql_update = f"INSERT INTO twitter_followers (id, date, follower_count, change_followers) VALUES " \
                 f"(null, '{today_dt}', {follower_count} , {last_count});"
    db.query_db(sql_update)

if __name__ == '__main__':
    get_followers()
