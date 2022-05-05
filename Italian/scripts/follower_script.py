import numpy as np
import pandas as pd
import tweepy
import requests
import os
from itertools import cycle
import json
import time
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
pool = cycle(apis)
cont = 0


def process_follow(users, screen_name):
    api = next(pool)
    ids = []
    global cont
    if cont >= 14:
        api = next(pool)
    try:
        for page in tqdm.tqdm(tweepy.Cursor(api.friends_ids, screen_name=screen_name).pages()):
            cont += 1
            ids.extend(page)
        target = [screen_name for _ in ids]
        follow = pd.DataFrame(list(zip(target, ids)), columns=["user", "followed_id"])
        follow = follow.merge(users, left_on="followed_id", right_on="id")[["user", "screen_name"]]
        follow.rename(columns={'screen_name': 'followed_user'}, inplace=True)
        return follow
    except tweepy.error.TweepError as e:
        print(f"Error: {e}")
        lst_name = [np.nan]
        lst_screen = [np.nan]
        return pd.DataFrame(list(zip(lst_screen, lst_name)), columns=["user", "screen_name"])


def load_users_df():
    users = pd.DataFrame()
    path_users = r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Laura\Tweets\users"
    for i in glob.glob(path_users + r"\users_*.csv"):
        df = pd.read_csv(i, lineterminator="\n", encoding="utf-8", low_memory=False)
        users = users.append(df)
        users.reset_index(drop=True, inplace=True)
        users.drop_duplicates(subset=['id'], keep="last", inplace=True)
    return users[["id", "screen_name"]]


if __name__ == '__main__':
    # tweepy.debug(True)
    users = load_users_df()
    users_to_score = pd.read_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Laura\Files\to_follow.csv",
                                 lineterminator="\n", low_memory=False, encoding="utf-8")
    df = pd.DataFrame()
    for i in tqdm.tqdm(users_to_score["name"]):
        df = df.append(process_follow(users, i))
        print(f"{i} parsed.")
    df.to_csv(fr"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Laura\Files\user_follow\followers.csv",
              line_terminator="\n", index=False, encoding="utf-8")
