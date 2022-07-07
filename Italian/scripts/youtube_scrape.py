import urllib.request
import numpy as np
from tqdm.auto import tqdm
import pandas as pd
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import urllib.parse as p
import re
import os
import pickle

workers = 3
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
api_service_name = "youtube"
api_version = "v3"
client_secrets_file = "client.json"

flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    client_secrets_file, scopes)
credentials = flow.run_console()
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, credentials=credentials)

def clean_url(df):
    print("Cleaning DataFrame and getting youtube links...")
    yt_df = df[(df["urls"].str.contains("https://youtu.be")) | (df["urls"].str.contains("https://youtube"))]
    screen_name = []
    lst_urls = []
    for row in tqdm(yt_df.itertuples()):
        url_exp = row.urls.split(" ")
        for exp in range(len(url_exp)):
            if url_exp[exp] == "'expanded_url':":
                lst_urls.append(url_exp[exp + 1][1:-2])
                screen_name.append(row.user_screen_name)
    print("DataFrame cleaned!")
    return pd.DataFrame(list(zip(screen_name, lst_urls)), columns=["user_screen_name", "link"])


def get_video_id_by_url(url):
    parsed_url = p.urlparse(url)
    video_id = p.parse_qs(parsed_url.query).get("v")
    if video_id:
        return video_id[0]
    else:
        raise Exception(f"Wasn't able to parse video URL: {url}")


def get_video_details(youtube, **kwargs):
    return youtube.videos().list(
        part="snippet,contentDetails,statistics",
        **kwargs
    ).execute()


def get_video_info(urls: list, count):
    print(f"Worker {count} started...")
    available_, duration_, title_, description_, views_, likes_, comments_, channel_, date_ = ([] for _ in range(9))
    for url in tqdm(urls):
        try:
            video_id = get_video_id_by_url(url)
            response = get_video_details(youtube, id=video_id)
            items = response.get("items")[0]
            snippet = items["snippet"]
            statistics = items["statistics"]
            content_details = items["contentDetails"]
            channel_title = snippet["channelTitle"]
            title = snippet["title"]
            description = snippet["description"]
            publish_time = snippet["publishedAt"]
            comment_count = statistics["commentCount"]
            like_count = statistics["likeCount"]
            view_count = statistics["viewCount"]
            duration = content_details["duration"]
            parsed_duration = re.search(f"PT(\d+H)?(\d+M)?(\d+S)", duration).groups()
            duration_str = ""
            for d in parsed_duration:
                if d:
                    duration_str += f"{d[:-1]}:"
            duration_str = duration_str.strip(":")
            title_.append(title)
            description_.append(description)
            channel_.append(channel_title)
            date_.append(publish_time)
            duration_.append(duration_str)
            comments_.append(comment_count)
            likes_.append(like_count)
            views_.append(view_count)
            available_.append(True)
        except Exception as e:
            print(e)
            available_.append(False)
            title_.append(np.nan)
            description_.append(np.nan)
            channel_.append(np.nan)
            date_.append(np.nan)
            duration_.append(np.nan)
            comments_.append(np.nan)
            likes_.append(np.nan)
            views_.append(np.nan)
    df = pd.DataFrame(list(zip(available_, duration_, title_, description_, views_, likes_, comments_, channel_, date_)),
                      columns=["available", "duration", "title", "description", "views", "likes", "comments", "channel",
                               "pubblication_date"])
    print(f"Worker {count} finished!")
    return df

def parse_yt_parallel(urls: list):
    results = pd.DataFrame()
    futures = []
    executor = ProcessPoolExecutor(max_workers=workers)
    sublist = np.array_split(urls, workers)
    count = 0
    for sc in sublist:
        futures.append(executor.submit(get_video_info, sc, count))
        count = count + 1
    futures_wait(futures)
    for fut in futures:
        results = pd.concat([results, fut.result()], axis=0)
    results.reset_index(drop=True, inplace=True)
    return results

if __name__ == '__main__':
    # tweets = pd.read_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\files\tweets\tweets.csv",
    #                      usecols=["user_screen_name", "urls"], lineterminator="\n", low_memory=False, encoding="utf-8")
    urls = ["https://www.youtube.com/watch?v=jgUk-uIz22U", "https://www.youtube.com/watch?v=AhyCEUZeWt8", "https://www.youtube.com/watch?v=vIOWzD_N7xk"]

    # urls = clean_url(tweets)
    # df = parse_yt_parallel(list(urls["link"]))
    df = parse_yt_parallel(urls)

    df.to_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\script_directory_output\youtube"
              r"\prova_yt.csv",
              line_terminator="\n", encoding="utf-8", index=False)
