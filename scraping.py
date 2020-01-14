# This is Main function.
# Extracting streaming data from Twitter, pre-processing, and loading into Postgre

import credentials  # Import api/access_token keys from credentials.py
import settings  # Import related setting constants from settings.py
import psycopg2
import re
import time
from opencage.geocoder import OpenCageGeocode
from opencage.geocoder import RateLimitExceededError
import tweepy
import pandas as pd
from textblob import TextBlob

selectedOpencageApiKey = 1


def analyze(tweet_text):
    matching = ''
    matchingCnt = 0

    for key in settings.TRACK_WORDS:
        if tweet_text.count(key) > matchingCnt:
            matching = key
            matchingCnt = tweet_text.count(key)

    return matching.capitalize()


class Geocoder:
    # def __init__(self):
    #     global selectedOpencageApiKey
    #     self.geocoder = settings.OpencageApiKeys[selectedOpencageApiKey]

    def forward_geocode(self, location):
        global selectedOpencageApiKey
        print(selectedOpencageApiKey)
        geocoder = OpenCageGeocode(settings.OpencageApiKeys[selectedOpencageApiKey])
        selectedOpencageApiKey = (selectedOpencageApiKey + 1) % 3;
        results = geocoder.geocode(location)
        if len(results) > 0:
            return results[0]['geometry']['lat'], results[0]['geometry']['lng']
        return None, None


# Override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    '''
    Tweets are known as “status updates”. So the Status class in tweepy has properties describing the tweet.
    https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
    '''

    def __init__(self):
        super().__init__()
        self.time = time.time()
        self.geocoder = Geocoder()
        self.lastTime = time.time()

    def get_data(self, data):
        if data.id_str is None:
            return None

        if data.retweeted and 'RT @' not in data.text:
            return None

        # Extract attributes from each tweet
        id_str = data.id_str
        created_at = data.created_at
        text = deEmojify(data.text)
        # if data['extended_tweet']:
        #      print(data['extended_tweet'])
        #      text = deEmojify(data['extended_tweet'].full_text)
        sentiment = TextBlob(text).sentiment
        polarity = sentiment.polarity
        subjectivity = sentiment.subjectivity
        match = analyze(text)

        if len(match) == 0:
            return None

        user_created_at = data.user.created_at
        user_location = deEmojify(data.user.location)
        user_description = deEmojify(data.user.description)
        user_followers_count = data.user.followers_count
        retweet_count = data.retweet_count
        favorite_count = data.favorite_count
        longitude = None
        latitude = None

        if data.coordinates:
            longitude = data.coordinates['coordinates'][0]
            latitude = data.coordinates['coordinates'][1]
        elif data.place is not None:
            latitude, longitude = self.geocoder.forward_geocode(
                data.place.full_name + "," + data.place.country
            )
        elif data.user.location is not None:
            latitude, longitude = self.geocoder.forward_geocode(
                data.user.location
            )
        if latitude is not None and longitude is not None:
            return {
                "id_str": id_str,
                "created_at": created_at,
                "text": text,
                "polarity": polarity,
                "subjectivity": subjectivity,
                "user_created_at": user_created_at,
                "user_location": user_location,
                "user_description": user_description,
                "user_followers_count": user_followers_count,
                "longitude": longitude,
                "latitude": latitude,
                "retweet_count": retweet_count,
                "favorite_count": favorite_count,
                "kmatch": match
            }
        return None

    def on_status(self, data):
        '''
        Extract info from tweets
        '''
        currTime = time.time()

        # print(currTime - self.lastTime)
        if currTime - self.lastTime < 1:
            return
        self.lastTime = currTime
        dataToInsert = self.get_data(data)
        # delete data from postgress
        cur = conn.cursor()
        # delete_query = '''
        #         DELETE FROM {0}
        #         WHERE id_str IN (
        #             SELECT id_str
        #             FROM {0}
        #             ORDER BY created_at asc
        #             LIMIT 200) AND (SELECT COUNT(*) FROM {0}) > 9600;
        #         '''.format(settings.TABLE_NAME)
        #
        # cur.execute(delete_query)
        # conn.commit()

        # print(dataToInsert["kmatch"])
        # insert into postgress database
        if (dataToInsert is not None):
            sql = "INSERT INTO {} (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count, kmatch) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                settings.TABLE_NAME)
            val = (dataToInsert["id_str"], dataToInsert["created_at"], dataToInsert["text"], dataToInsert["polarity"],
                   dataToInsert["subjectivity"], dataToInsert["user_created_at"], dataToInsert["user_location"],
                   dataToInsert["user_description"], dataToInsert["user_followers_count"], dataToInsert["longitude"],
                   dataToInsert["latitude"], dataToInsert["retweet_count"], dataToInsert["favorite_count"],
                   dataToInsert["kmatch"])
            cur.execute(sql, val)
            conn.commit()
            self.time = time.time()
            print(dataToInsert["id_str"])
        cur.close()

    def on_error(self, status_code):
        '''
        Since Twitter API has rate limits, stop srcraping data as it exceed to the thresold.
        '''
        if status_code == 420:
            # return False to disconnect the stream
            return False


def clean_tweet(self, tweet):
    ''' 
    Use sumple regex statemnents to clean tweet text by removing links and special characters
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                |(\w+:\/\/\S+)", " ", tweet).split())


def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None


# print(connection.mflix)
DATABASE_URL = settings.BETA_DATABASE_URL
# DATABASE_URL = settings.DEV_DATABASE_URL

# DATABASE_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(DATABASE_URL, sslmode='require')
cur = conn.cursor()

while True:
    try:
        auth = tweepy.OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
        auth.set_access_token(credentials.ACCESS_TOEKN, credentials.ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth)

        myStreamListener = MyStreamListener()
        myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener, tweet_mode='extended')
        myStream.filter(languages=["en"], track=settings.TRACK_WORDS, stall_warnings=True)
        print("Ok")
    except Exception as ex:
        print("######Error######\n")
        print(ex)
        if (ex is RateLimitExceededError):
            selectedOpencageApiKey = (selectedOpencageApiKey + 1) % 3

    finally:
        print("######Continue######\n")

# Close the MySQL connection as it finished
# However, this won't be reached as the stream listener won't stop automatically
# Press STOP button to finish the process.
conn.close()
