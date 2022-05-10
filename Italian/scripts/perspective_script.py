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
    results = pd.DataFrame()
    executor = ProcessPoolExecutor(max_workers=3)
    sublist = np.array_split(texts, 3)
    count = 0
    keys = get_credentials()
    for sc in sublist:
        futures.append(executor.submit(score, keys[count], sc, count))
        count = count + 1
    futures_wait(futures)
    for fut in futures:
        results = results.append(fut.result())
    results.reset_index(drop=True, inplace=True)
    return results

def get_credentials():
    jsonFile = open(r'C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\Files\auth.json', 'r')
    config = json.load(jsonFile)
    jsonFile.close()
    keys = []

    for i in range(len(config)):
        keys.append(config[f"api_key_{i}"])
    return keys

def score(API_KEY: str, text: list, count: int):
    print(f"Worker {count} started!")
    content = []
    score = []
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
            score.append(response["attributeScores"]["TOXICITY"]["summaryScore"]["value"])
            content.append(i)

        except Exception as e:
            print("Error: ", e)
            print(f"On text:{i}")
        time.sleep(1)
    print(f"Shutting down worker {count}...")
    df = pd.DataFrame(list(zip(content,score)), columns=["text","toxicity"])
    return df

if __name__ == '__main__':
    contents = pd.read_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian"
                           r"\script_directory_output\example_10.csv", lineterminator="\n", low_memory=False,
                           encoding="utf-8")["text"]
    res = parallel_execution(contents)
    res.to_csv(r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\script_directory_output\results_10"
               r".csv", line_terminator="\n", encoding="utf-8", index=False)
    print("Finished!")
