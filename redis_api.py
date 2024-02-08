'''
Sarah Klute
DS 4300 HW2 Redis Implimentation
Redis API for implimentation in redis_tester.py
'''
from tweets_objects import Tweet, Follows
import pandas as pd
from datetime import datetime
import time
import redis
import csv
import random

class TwitterAPI:

    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    def load_follows_data(self, csv_file_path):
        '''loading in teh follows data from csv into the redis database'''
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row

            for row in csv_reader:
                user_id, follows_id = map(int, row)

                # Add follows_id to the followers of user_id
                self.redis_client.sadd(f'Followers: {user_id}', follows_id)

                # Add user_id to the following list of follows_id
                self.redis_client.sadd(f'Following: {follows_id}', user_id)


    def post_tweet(self, tweet):
        '''Posting tweet individually into Redis'''

        # Auto increment the tweet_id
        tweet.tweet_id = self.redis_client.incr('tweet_id')

        # Set tweet timestamp
        tweet.tweet_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Tweet data as a dictionary
        tweet_data = {
            'tweet_id': tweet.tweet_id,
            'user_id': tweet.user_id,
            'tweet_text': tweet.tweet_text,
            'tweet_ts': tweet.tweet_ts
        }

        # Store tweet details in a hash with tweet_id as key
        self.redis_client.hset(f'Tweet:{tweet.tweet_id}', mapping=tweet_data)

        # Add tweet to the user's home timeline
        self.redis_client.lpush(f'User {tweet.user_id} HomeTimeline:', tweet.tweet_id)


    def home_timeline(self, user_id, num_tweets=10):
        '''fetching a users hometimeline of their followers 10 most recent tweets'''
        # Users followers
        followers_ids = self.redis_client.smembers(f'Followers: {user_id}')

        # Get most recent tweets from the users followers
        timeline_tweets = []
        for followers_id in followers_ids:
            followers_tweets = self.redis_client.lrange(f'User {followers_id} HomeTimeline:', 0, num_tweets - 1)
            timeline_tweets.extend(followers_tweets)

        # Sort and get the latest num_tweets
        timeline_tweets = sorted(timeline_tweets, key=lambda x: int(x), reverse=True)[:num_tweets]

        # Get tweet details from redis
        tweet_details = [
            self.redis_client.hmget(f'Tweet:{tweet_id}', ['tweet_id', 'user_id', 'tweet_text', 'tweet_ts'])
            for tweet_id in timeline_tweets
        ]

        # Create dataframe of tweet details to show hometimeline
        df = pd.DataFrame(
            tweet_details,
            columns=['tweet_id', 'user_id', 'tweet_text', 'tweet_ts']
        )

        return df

    def get_all_tweet_ids(self):
        '''get all tweet_ids in redis'''
        # Initiate
        cursor = '0'
        tweet_ids = []
        # Get the tweets through cursor and add to tweet_id list
        cursor, key_batch = self.redis_client.scan(cursor, match="Tweet:*", count=20)
        tweet_ids.extend(key_batch)
        return tweet_ids

    def get_random_user_id(self):
        '''get random user id from all users loaded'''
        # Get all unique user IDs from tweet details
        timeline_tweets = self.get_all_tweet_ids()
        user_ids = set()

        # for every tweet get user_id within tweet_data
        for tweet_id in timeline_tweets:
            tweet_data = self.redis_client.hmget(f'{tweet_id}', ['user_id'])
            user_id = tweet_data[0] if tweet_data[0] else None
            if user_id:
                user_ids.add(user_id)
        user_id_ls = list(user_ids)

        # Return a random user ID
        return random.choice(user_id_ls)

    def calculate_timelines_per_second(self, num_users):
        '''calculating the timeliens per second given the number of users'''
        random_user_id = self.get_random_user_id()
        if not random_user_id:
            print("No users available to simulate a timeline.")
            return
        self.home_timeline(random_user_id)

        # Measure time for multiple users
        start_time = time.time()

        for _ in range(num_users):
            # Retrieve home timeline for random user
            self.home_timeline(random_user_id)

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Print the sample timeline
        sample_timeline = self.home_timeline(random_user_id)
        print(f"\nSample Timeline for User {random_user_id}:\n{sample_timeline}\n")
        print(f"Number of timelines retrieved per second: {num_users / elapsed_time:.2f}")
        print(f"Time taken to retrieve timelines for {num_users} users: {elapsed_time:.2f} seconds")

