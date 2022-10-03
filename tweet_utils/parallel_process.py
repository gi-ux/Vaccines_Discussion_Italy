from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
from tqdm import tqdm
import numpy as np
import re
import string
import it_core_news_lg
import pandas as pd

path_files = r"C:\Users\gianluca.nogara\Desktop\Repo\Vaccines_Discussion_Italy\Italian\files\tweets"
nlp = it_core_news_lg.load()
workers = 10


def lemmatizer(text):
    lem = []
    doc = nlp(text)
    for word in doc:
        lem.append(word.lemma_)
    return " ".join(lem)


def clean_text(text):
    # Normalize Text
    text = text.lower()
    # Remove Unicode Characters
    text = re.sub(r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", '', text)
    # Remove Punctuaction
    text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text)
    # Remove words containing numbers
    text = re.sub(r'\w*\d\w*', '', text)
    # Remove Stopwords
    all_stop_words = nlp.Defaults.stop_words
    text = " ".join([word for word in str(text).split() if word not in all_stop_words])
    # Lemmatization
    text = lemmatizer(text)
    text = re.sub(r'-PRON-', '', text)
    return text


def process_users(lst: list):
    futures = []
    results = []
    executor = ProcessPoolExecutor(max_workers=10)
    sublist = np.array_split(lst, 10)
    count = 0
    for sc in sublist:
        futures.append(executor.submit(execute, sc, count))
        count += 1
    futures_wait(futures)
    for fut in futures:
        results.extend(fut.result())
    return results


def execute(lst: list, count):
    res = []
    print(f"Worker {count} started...")
    for i in tqdm(lst):
        res.append(clean_text(i))
    print(f"Worker {count} finished!")
    return res


if __name__ == '__main__':
    df = pd.read_csv(path_files + r"\tweets.csv", lineterminator="\n", low_memory=False, encoding="utf-8",
                     usecols=["user_screen_name", "text", "hashtags", "urls"])
    lst = process_users(list(df["text"]))
    df["text_cleaned"] = lst
    df.to_csv(path_files + r"\df_NLP.csv", line_terminator="\n", index=False, encoding="utf-8")
