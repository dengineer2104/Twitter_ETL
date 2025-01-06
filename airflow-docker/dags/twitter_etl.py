import pandas as pd
import json
from datetime import datetime
import s3fs
from twarc import Twarc
import tweepy

access_token = "WKjjQ9YGIwEKhb2KB2kQGNnjJ"
access_token_secret = "In63pKJojkCz91yB2Wo4Zst11UDAC1v1mt8FCl1YQThTLp2DBc"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAGYsxwEAAAAA%2F8l0y76xznGgOVPZDDweB7rQLzQ%3DPBYQeSSqDvvl1gPJs6YOIKjResOpvcQBoRdC39L44iO6zdOsBL"
consumer_key = "880113668351868928-xgYk6UToJrNhHHpno8JMoJL3iW3nolq"
consumer_secret = "DUoyMGHlJqs3ohTIpeHJEX2LOpmYiEZFQ9M7pgoPY88B4"


# Initialize the Tweepy client
client = tweepy.Client(bearer_token=bearer_token)
api = tweepy.API(client, wait_on_rate_limit=True)

def twitter_etl(username):
    # Initialize a list to hold the tweet data
    tweet_list = []

    try:
        # Get the user ID using the username
        user = client.get_user(username=username)
        user_id = user.data.id  # Extract the user ID

        # Fetch tweets using the user ID
        tweets = client.get_users_tweets(id=user_id, max_results=100)  # Max 100 tweets per request

        if tweets.data:
            for tweet in tweets.data:
                # Check if public_metrics exists before accessing
                public_metrics = tweet.public_metrics if tweet.public_metrics else {}

                # Refine the tweet data
                refined_tweet = {
                    "user": username,
                    "text": tweet.text,
                    "favorite_count": public_metrics.get('like_count', 0),
                    "retweet_count": public_metrics.get('retweet_count', 0),
                    "created_at": tweet.created_at
                }
                tweet_list.append(refined_tweet)

        else:
            print(f"No tweets found for {username}")

    except tweepy.errors.Forbidden as e:
        print(f"Error fetching tweets for {username}: Forbidden - {e}")
    except tweepy.errors.Unauthorized as e:
        print(f"Error fetching tweets for {username}: Unauthorized - {e}")
    except tweepy.errors.TweepyException as e:
        print(f"Error fetching tweets for {username}: {e}")

    # Convert the list of tweets to a pandas DataFrame
    df = pd.DataFrame(tweet_list)

    # Save the DataFrame to a CSV file
    df.to_csv(f"{username}_tweets.csv", index=False)
    print(f"Tweets saved to {username}_tweets.csv")

# Fetch tweets for a user (e.g., @shakira)
twitter_etl("shakira")
