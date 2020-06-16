import tweepy
import time
import json
import twitter_keys
from datetime import datetime
from pytz import timezone, utc
import sys, os

class TwitterCrawler():
    def __init__(self, keys, folder_filename):
        self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret = keys[0], keys[1], keys[2], keys[3]
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(self.auth)
        self.folder_file = folder_filename
        self.function == None


    def _limit_handled(self, iterator):
        while True:
            try:
                yield iterator.next()
            except (tweepy.RateLimitError, tweepy.error.TweepError):
                seconds = 15 * 60

                print("")
                print("Sleeping for", seconds / 60, "minutes")
                time.sleep(seconds)
            except StopIteration:
                return

    def get_users_timeline(self, user_ids):
        self.function = "timeline"
        num_users = 0

        for id in user_ids:
            num_users += 1

            file = f"{self.folder_file}/timeline/{id}.csv"

            if os.path.exists(file):
                reader = open(file, "r", encoding="utf8")
                line = reader.readline()
                jtweet = json.loads(line)
                since_id = jtweet["id"]
                reader.close()

                writer = open(file, "a", encoding="utf8")
                tweets_iterator = tweepy.Cursor(self.api.user_timeline, user_id=id, since_id=since_id).items()
            else:
                writer = open(file, "w", encoding="utf8")
                tweets_iterator = tweepy.Cursor(self.api.user_timeline, user_id=id).items()

            total_tweet = 0

            for tweet in self._limit_handled(tweets_iterator):
                total_tweet += 1

                jtweet = tweet._json

                created_date = jtweet["created_at"]
                earliest_date = datetime.strptime("Wed Jan 01 00:00:00 +0800 2020", "%a %b %d %H:%M:%S %z %Y")
                dat = datetime.strptime(created_date, "%a %b %d %H:%M:%S %z %Y")
                dat = dat.replace(tzinfo=utc).astimezone(tz=timezone("Asia/Singapore"))
                if dat < earliest_date:
                    break

                writer.write(json.dumps(jtweet))
                writer.write("\n")
                writer.flush()

            writer.close()

            print("\r", end="")
            print("Users scraped:", f"{num_users}/{len(user_ids)}", end="", flush=True)



    def search(self, keyword, since_id):
        self.function = "search"
        writer = open(f"{self.folder_file}/tweets/sgunited_1.csv", "w", encoding="utf8")

        # result_type 'recent' returns 0 tweets if there is no recent tweets
        if since_id == -1:
            tweets_iterator = tweepy.Cursor(self.api.search, q=keyword).items()
        else:
            tweets_iterator = tweepy.Cursor(self.api.search, q=keyword, since_id=since_id).items()

        total_tweet = 0

        for tweet in self._limit_handled(tweets_iterator):
            total_tweet += 1

            jtweet = tweet._json

            writer.write(json.dumps(jtweet))
            writer.write("\n")
            writer.flush()

            print("\r", end="")
            print("Total tweet:", total_tweet, end="", flush=True)

        writer.close()

def get_user_ids (file):

    with open(file, "r", encoding="utf8") as reader:
        lines = reader.readlines()

    ids = [id.strip() for id in lines]

    return ids


if __name__ == "__main__":
    twit_crawl = TwitterCrawler([twitter_keys.n4j2_ck, twitter_keys.n4j2_cs, twitter_keys.n4j2_at, twitter_keys.n4j2_ats],
                                "sgunited")

    #user_ids = get_user_ids("../covid19/seed_user_ids.txt")
    user_ids = get_user_ids("seed_user_ids.txt")
    #twit_crawl.search("#SGUnited", 1272443497023328256)

    twit_crawl.get_users_timeline(user_ids)

