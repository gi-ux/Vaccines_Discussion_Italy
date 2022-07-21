import numpy as np
import pandas as pd
import tweepy
from itertools import cycle
import json
import tqdm
import glob
import warnings

warnings.simplefilter("ignore")

jsonFile = open('C:/Users/gianl/Desktop/Gi/Supsi/twitterdatamanager/credentials.json', 'r')
config = json.load(jsonFile)
jsonFile.close()

consumer_keys = []
consumer_secrets = []
access_tokens = []
access_token_secrets = []
apis = []

for i in config["DEFAULT"]["twitter_credentials"]:
    consumer_keys.append(i["consumer_key"])
    consumer_secrets.append(i["consumer_secret"])
    access_tokens.append(i["access_token"])
    access_token_secrets.append(i["access_token_secret"])

    auth = tweepy.OAuthHandler(i["consumer_key"], i["consumer_secret"])
    auth.set_access_token(i["access_token"], i["access_token_secret"])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    apis.append(api)
print(f"{len(apis)} APIs generated")
pool = cycle(apis)
cont = 0


def process_followers(screen_name):
    api = next(pool)
    name = []
    followers = []
    global cont
    if cont >= 14:
        api = next(pool)
    try:
        for item in tqdm.tqdm(tweepy.Cursor(api.followers, screen_name=screen_name).items()):
            cont += 1
            name.append(screen_name)
            followers.extend(item.screen_name)
    except tweepy.error.TweepError as e:
        print(f"Error: {e}")
        name = [np.nan]
        followers = [np.nan]
        return pd.DataFrame(list(zip(name, followers)), columns=["user", "screen_name"])


def load_users_df():
    path = r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\files\Tweets\tweets.csv"
    tweets = pd.read_csv(path, lineterminator="\n", low_memory=False, encoding="utf-8", usecols=["user_screen_name"])
    df = tweets.groupby("user_screen_name")["user_screen_name"].count().reset_index(name="count")
    df = df[df["count"] >= 10]
    print(f"{len(df)} users to be processed")
    return df

if __name__ == '__main__':
    users = load_users_df()
    df = pd.DataFrame()
    for i in tqdm.tqdm(users["screen_name"]):
        df = pd.concat([df, process_followers(i)], axis=0)
        print(f"{i} parsed")
    df = df[df["screen_name"].isin(list(users["screen_name"]))]
    df.to_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\script_directory_output"
              r"\followers_output\followers.csv", line_terminator="\n", index=False, encoding="utf-8")
