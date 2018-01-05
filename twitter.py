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
    with pymysql.connect(host=host, user=user, password=password, db=database) as cursor:
        # Write update to twitter_followers
        # Get most recent follower count
        cursor.execute("SELECT follower_count FROM twitter_followers "
                       "WHERE DATE IN (SELECT MAX(date) FROM twitter_followers);")
        result = cursor.fetchone()
        # Calculate change in followers since last period
        last_count = follower_count - result[0]
        # Add new row and commit
        sql = f"INSERT INTO twitter_followers(id, date, follower_count, change_followers) " \
              f"VALUES(null, '{today_dt}', {follower_count}, {last_count});"
        try:
            cursor.execute(sql)
        except pymysql.Error as e:
            cursor.rollback()


# Get keys
with open('apikeys.txt', 'r') as f:
    keys = f.read().splitlines()

consumer_key, consumer_secret, access_token, access_secret = keys

# Get auth details + date
with open('server-permissions.txt', 'r') as f:
    s_keys = f.read().splitlines()

host, user, password, database = s_keys


if __name__ == '__main__':
    get_followers()
