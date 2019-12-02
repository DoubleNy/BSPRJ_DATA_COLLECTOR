TRACK_WORDS = ['Facebook', 'google', 'amazon', 'elon musk']
TABLE_NAME = "DATA_COLLECTION"
TABLE_ATTRIBUTES = "id_str varchar(255), created_at date, text varchar(255), \
            polarity integer, subjectivity integer, user_created_at varchar(255), user_location varchar(255), \
            user_description varchar(255), user_followers_count integer, longitude double, latitude double, \
            retweet_count integer, favorite_count integer"