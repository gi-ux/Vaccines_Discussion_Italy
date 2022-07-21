from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
from tqdm import tqdm
import numpy as np
import pandas as pd
import json
import glob
import os
import botometer
import warnings

warnings.filterwarnings("ignore")


def init_credentials():
    jsonFile = open(r'C:\Users\gianluca.nogara\Desktop\Repo\twitterdatamanager\credentials.json', 'r')
    config = json.load(jsonFile)
    jsonFile.close()
    keys = []
    for i in config["DEFAULT"]["rapidapi_credentials"]:
        keys.append(i["key"])
    boms = []
    for i in range(len(keys)):
        consumer_key = config["DEFAULT"]["twitter_credentials"][i]["consumer_key"]
        consumer_secret = config["DEFAULT"]["twitter_credentials"][i]["consumer_secret"]
        access_token_secret = config["DEFAULT"]["twitter_credentials"][i]["access_token_secret"]
        access_token = config["DEFAULT"]["twitter_credentials"][i]["access_token"]
        twitter_app_auth = {
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret,
            'access_token': access_token,
            'access_token_secret': access_token_secret,
        }
        boms.append(botometer.Botometer(wait_on_ratelimit=True,
                                        rapidapi_key=keys[i],
                                        **twitter_app_auth))
    print(len(boms))
    return boms


def score(names, bom, count):
    result = []
    print(f"Start thread {count}")
    for name in tqdm(names):
        try:
            result.append(bom.check_account('@' + name))
        except Exception as e:
            result.append(e)
    print(f"Result of thread number {count}")
    pd.DataFrame(list(zip(names, result)), columns=["name", "score"]).to_csv(
        fr"..\script_directory_output\bom\score_bom_{count}.csv",
        line_terminator="\n", encoding="utf-8", index=False)


def process_users(urls: list):
    futures = []
    executor = ProcessPoolExecutor(max_workers=15)
    sublist = np.array_split(urls, 15)
    credentials = init_credentials()
    count = 0
    for sc in sublist:
        futures.append(executor.submit(score, sc, credentials[count], count))
        count += 1
    futures_wait(futures)


def read_file():
    df = pd.DataFrame()
    for path in glob.glob(r"..\script_directory_output\suspended_users\out*.csv"):
        temp = pd.read_csv(path, lineterminator="\n", encoding="utf-8", low_memory=False)
        temp = temp[temp["status"] == "ok"]
        df = df.append(temp)
        df.drop_duplicates(subset=["name"], inplace=True)
    names = list(df["name"])[72000:79000]
    return names


def clear_results():
    res = pd.DataFrame()
    for path in tqdm(glob.glob(r"..\script_directory_output\bom\score_*.csv")):
        temp = pd.read_csv(path, lineterminator="\n", encoding="utf-8", low_memory=False)
        res = res.append(temp)
        os.remove(path)
    res.reset_index(drop=True, inplace=True)
    res.to_csv(r"..\script_directory_output\bom\result_25.csv", line_terminator="\n", encoding="utf-8", index=False)


if __name__ == '__main__':
    names = read_file()
    process_users(names)
    clear_results()
    print("Completed parsing")
