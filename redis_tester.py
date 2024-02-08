"""
Sarah Klute
DS 4300 HW2 Redis Twitter Implimentation
Main function, calling tweets_redis_connect.py and tweet_objects.py
"""
from redis_api import TwitterAPI
import pandas as pd
from tweets_objects import Tweet, Follows
import time

def main():

    # Authenticate
    api = TwitterAPI(host='localhost', port=6379, db=0)
    api.redis_client.flushall()

    # Load data from CSV into FOLLOWS table
    api.load_follows_data('follows.csv')
    print("Follows data loaded into Redis.")

    # Record start time
    start_time = time.time()

    # Read Tweet CSV file into a pandas DataFrame
    df_TWEET = pd.read_csv('tweet.csv')

    # Iterate over each row in the TWEETS df
    for index, row in df_TWEET.iterrows():
        twt = Tweet(tweet_id = None, user_id=row[0], tweet_ts = None, tweet_text = row[1])
        api.post_tweet(twt)

    # Calculating time to load 10000000 tweets into table
    end_time = time.time()
    elapsed_time = end_time - start_time
    # Avg tweets per second
    total_tweets = len(df_TWEET)
    average_tweets_per_second = total_tweets / elapsed_time

    print(f"Number of tweets processed: {total_tweets}")
    print(f"Time taken to process tweets: {elapsed_time:.2f} seconds")
    print(f"Average Tweets per Second: {average_tweets_per_second:.2f}")

    # Set the display option to show all columns
    pd.set_option('display.max_columns', None)

    # Random user home timeline
    num_users_to_fetch = 500
    # Returns time calculations of home_timeline and sample of random user hometimeline
    api.calculate_timelines_per_second(num_users_to_fetch)

if __name__ == '__main__':
    main()