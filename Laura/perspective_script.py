import time
import numpy as np
import pandas as pd
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
import json

import tqdm
from googleapiclient import discovery

def parallel_execution(texts: list):
    futures = []
    results = []
    executor = ProcessPoolExecutor(max_workers=3)
    sublist = np.array_split(texts, 3)
    count = 0
    keys = get_credentials()
    for sc in sublist:
        futures.append(executor.submit(score, keys[count], sc, count))
        count = count + 1
    futures_wait(futures)
    for fut in futures:
        results.extend(fut.result())
    return results

def get_credentials():
    jsonFile = open('./Files/auth.json', 'r')
    config = json.load(jsonFile)
    jsonFile.close()
    keys = []

    for i in range(len(config)):
        keys.append(config[f"api_key_{i}"])
    return keys

def score(API_KEY: str, text: list, count: int):
    print(f"Worker {count} started!")
    scores = []
    for i in tqdm.tqdm(text):
        client = discovery.build(
            "commentanalyzer",
            "v1alpha1",
            developerKey=API_KEY,
            discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
            static_discovery=False,
        )

        analyze_request = {
            'comment': {'text': i},
            'requestedAttributes': {'TOXICITY': {}}
        }
        try:
            response = client.comments().analyze(body=analyze_request).execute()
            scores.append((i, response["attributeScores"]["TOXICITY"]["summaryScore"]["value"]))
        except Exception as e:
            print("Error: ", e)
            print(f"On text:{i}")
        time.sleep(1)
    print(f"Shutting down worker {count}...")
    return scores

if __name__ == '__main__':
    contents = pd.read_csv("./texts/example.csv", lineterminator="\n", low_memory=False, encoding="utf-8")["text"]
    scores = parallel_execution(contents)
    for i in scores:
        print("Content: ")
        print(i)