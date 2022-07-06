import numpy as np
import pandas as pd
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
import json
import time
import tqdm
from googleapiclient import discovery


def parallel_execution(texts: list):
    futures = []
    results = pd.DataFrame()
    executor = ProcessPoolExecutor(max_workers=4)
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
    SEXUALLY_EXPLICIT = []
    INSULT = []
    PROFANITY = []
    TOXICITY = []
    LIKELY_TO_REJECT = []
    THREAT = []
    IDENTITY_ATTACK = []
    SEVERE_TOXICITY = []
    scores = [SEXUALLY_EXPLICIT, INSULT, PROFANITY, TOXICITY, LIKELY_TO_REJECT,
              THREAT, IDENTITY_ATTACK, SEVERE_TOXICITY]

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
            'requestedAttributes': {
                # 'TOXICITY': {},
                'SEVERE_TOXICITY': {},
                'IDENTITY_ATTACK': {},
                # 'SEXUALLY_EXPLICIT': {},
                # 'LIKELY_TO_REJECT': {},
                'INSULT': {},
                'PROFANITY': {},
                "THREAT": {}
            }
        }
        try:
            response = client.comments().analyze(body=analyze_request).execute()
            cont = 0
            for values in response["attributeScores"]:
                # print(values)
                for score in response["attributeScores"][values]:
                    for sum_score in response["attributeScores"][values][score]:
                        if sum_score == "value":
                            # print(response["attributeScores"][values][score][sum_score])
                            scores[cont].append(response["attributeScores"][values][score][sum_score])
                            cont += 1

            content.append(i)

        except Exception as e:
            print("Error: ", e)
            print(f"On text:{i}")
        time.sleep(1)
    print(f"Shutting down worker {count}...")
    df = pd.DataFrame(list(zip(content, SEXUALLY_EXPLICIT, INSULT, PROFANITY, TOXICITY, LIKELY_TO_REJECT,
                               THREAT, IDENTITY_ATTACK, SEVERE_TOXICITY)), columns=["text",
                                                                                    # "SEXUALLY_EXPLICIT",
                                                                                    "INSULT",
                                                                                    "PROFANITY",
                                                                                    # "TOXICITY",
                                                                                    # "LIKELY_TO_REJECT",
                                                                                    "THREAT",
                                                                                    "IDENTITY_ATTACK",
                                                                                    "SEVERE_TOXICITY"])
    return df


if __name__ == '__main__':
    path = r"C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\script_directory_output\toxic_texts" \
           r"\toxic_multiscore_diego"
    contents = pd.read_csv(path + r"\testo_0.csv", lineterminator="\n", low_memory=False, encoding="utf-8")["text"]
    res = parallel_execution(contents)
    res.to_csv(path + r"\result_0.csv", line_terminator="\n", encoding="utf-8", index=False)
    print("Finished!")
