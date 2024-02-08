from datetime import datetime

class Tweet:

    def __init__(self, tweet_id, user_id, tweet_ts, tweet_text):
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.tweet_ts = tweet_ts if tweet_ts else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.tweet_text = tweet_text

class Follows:

    def __init__(self, user_id, follows_id):
        self.user_id = user_id
        self.follows_id = follows_id

    def __str__(self):
        return f"Follows: {self.user_id}  ({self.follows_id})"

