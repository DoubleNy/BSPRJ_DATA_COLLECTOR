TRACK_WORDS = ['Headache', 'ILL', 'ACHE']
TABLE_NAME = "\"DATA_COLLECTION\""
TABLE_ATTRIBUTES = "id_str varchar(255), created_at date, text varchar(255), \
            polarity integer, subjectivity integer, user_created_at varchar(255), user_location varchar(255), \
            user_description varchar(255), user_followers_count integer, longitude double, latitude double, \
            retweet_count integer, favorite_count integer"

DATABASE_URL = "postgres://gykdutoutwmbmu:58fe951784b1417c80fd04e1adf0e21cfef7d3c5c954cba309e5f241d94e0ba2@ec2-46-137-113-157.eu-west-1.compute.amazonaws.com:5432/dc7gsfpk9oef97"