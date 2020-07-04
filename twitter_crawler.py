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


    def _limit_handled(self, iterator):
        #iterator gets ALL results
        while True:
            try:
                yield iterator.next()
            except tweepy.RateLimitError as e:
                seconds = 15 * 60

                print("")
                print("Sleeping for", seconds / 60, "minutes")
                time.sleep(seconds)
            except tweepy.TweepError as e:
                print("")
                print(e)
                return
            except StopIteration:
                return

    def get_users_timeline(self, user_ids):
        num_users = 0

        for id in user_ids:
            num_users += 1

            file = f"{self.folder_file}/timeline/{id}.csv"

            tweets_iterator = None

            write_mode = "w"

            if os.path.exists(file):
                reader = open(file, "r", encoding="utf8")
                lines = reader.readlines()

                if len(lines) > 0:
                    jtweet1 = json.loads(lines[0])
                    since_id1 = jtweet1["id"]
                    created_date1 = jtweet1["created_at"]

                    jtweet2 = json.loads(lines[len(lines)-1])
                    since_id2 = jtweet2["id"]
                    created_date2 = jtweet2["created_at"]

                    dat1 = datetime.strptime(created_date1, "%a %b %d %H:%M:%S %z %Y")
                    dat2 = datetime.strptime(created_date2, "%a %b %d %H:%M:%S %z %Y")

                    #tzinfo is time zone info. Assigning none to it means making it static
                    #so that the two times can be compared.
                    if dat1.replace(tzinfo=None) >= dat2.replace(tzinfo=None):
                        since_id = since_id1
                        dat = dat1.replace(tzinfo=None)
                    else:
                        since_id = since_id2
                        dat = dat2.replace(tzinfo=None)

                    dat = dat.replace(tzinfo=utc).astimezone(tz=timezone("Asia/Singapore"))
                    dat = dat.replace(tzinfo=None)
                    reader.close()

                    latest_date1 = datetime.strptime("Tue Jun 30 00:00:00 +0800 2020", "%a %b %d %H:%M:%S %z %Y")
                    latest_date2 = datetime.utcnow()
                    latest_date2 = latest_date2.replace(tzinfo=utc).astimezone(tz=timezone("Asia/Singapore"))
                    latest_date = min(latest_date1.replace(tzinfo=None), latest_date2.replace(tzinfo=None))

                    tweets_iterator = None
                    if (latest_date - dat).total_seconds() > 86400:
                        writer = open(file, "a", encoding="utf8")
                        write_mode = "a"
                        tweets_iterator = tweepy.Cursor(self.api.user_timeline, user_id=id, since_id=since_id, tweet_mode="extended").items()
            else:
                writer = open(file, "w", encoding="utf8")
                tweets_iterator = tweepy.Cursor(self.api.user_timeline, user_id=id, tweet_mode="extended").items()

            total_tweet = 0

            if tweets_iterator != None:
                for tweet in self._limit_handled(tweets_iterator):
                    total_tweet += 1

                    jtweet = tweet._json

                    created_date = jtweet["created_at"]
                    earliest_date = datetime.strptime("Wed Jan 01 00:00:00 +0800 2020", "%a %b %d %H:%M:%S %z %Y")
                    dat = datetime.strptime(created_date, "%a %b %d %H:%M:%S %z %Y")
                    dat = dat.replace(tzinfo=utc).astimezone(tz=timezone("Asia/Singapore"))

                    latest_date = datetime.strptime("Tue Jun 30 00:00:00 +0800 2020", "%a %b %d %H:%M:%S %z %Y")
                    if dat < earliest_date:
                        break

                    if dat <= latest_date:
                        writer.write(json.dumps(jtweet))
                        
                    writer.write("\n")
                    writer.flush()



                writer.close()

                print("\r", end="")
                print("Users scraped:", f"{num_users}/{len(user_ids)}", end="", flush=True)



    def search(self, keyword, since_id):
        writer = open(f"{self.folder_file}/tweets/sgunited_1.csv", "w", encoding="utf8")

        # result_type 'recent' returns 0 tweets if there is no recent tweets
        if since_id == -1:
            tweets_iterator = tweepy.Cursor(self.api.search, q=keyword, tweet_mode="extended").items()
        else:
            tweets_iterator = tweepy.Cursor(self.api.search, q=keyword, since_id=since_id, tweet_mode="extended").items()

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
    user_ids = get_user_ids("seed_user_ids1.txt")

    twit_crawl = TwitterCrawler(
        [twitter_keys.n4j3_ck, twitter_keys.n4j3_cs, twitter_keys.n4j3_at, twitter_keys.n4j3_ats],
        "sgunited")
    user_ids = get_user_ids("seed_user_ids2.txt")


    #twit_crawl.search("#SGUnited", 1277497286939959298)
    #twit_crawl.get_users_timeline(["358694898"])
    twit_crawl.get_users_timeline(user_ids)

