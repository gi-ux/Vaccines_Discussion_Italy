import numpy as np
import pandas as pd
import time
import tqdm
import glob
import warnings
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
from feel_it import EmotionClassifier, SentimentClassifier

warnings.simplefilter("ignore")
sentiment_classifier = SentimentClassifier()
emotion_classifier = EmotionClassifier()
num_workers = 8

def parallel_execution(texts: list):
    futures = []
    results = pd.DataFrame()
    executor = ProcessPoolExecutor(max_workers=num_workers)
    sublist = np.array_split(texts, num_workers)
    count = 0
    for sc in sublist:
        futures.append(executor.submit(sentiment, sc, count))
        count = count + 1
    futures_wait(futures)
    for fut in futures:
        results = results.append(fut.result())
    results.reset_index(drop=True, inplace=True)
    return results

def sentiment(texts: list, num):
    sent = []
    emotion = []
    print("Start worker ", num
          )
    for i in tqdm.tqdm(texts):
        sent.extend(sentiment_classifier.predict([i]))
        emotion.extend(emotion_classifier.predict([i]))
    df = pd.DataFrame(list(zip(texts, sent, emotion)), columns=["text", "sentiment", "emotion"])
    print("Ending worker ", num)
    return df

if __name__ == '__main__':
    lst = ["bene dai tutto ok io",
           "sisi oggi tutto bene",
           "guarda, sono parecchio incazzato ora",
           "oh non sopporto più questo scemo!",
           "ora niente non sto facendo nulla",
           "stasera penso che passerò una serata tranquilla",
           "bah, adesso ho perso la pazienza sai",
           "ho ricevuto un mesaggio, ora controllo"]
    df = parallel_execution(lst)
    print(df)