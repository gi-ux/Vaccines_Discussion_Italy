import pandas as pd
from tqdm.notebook import tqdm
import matplotlib.pyplot as plt
import numpy as np
import warnings
import time
import datetime

warnings.filterwarnings("ignore")


def split(df):
    original = df[df["in_reply_to_screen_name"].isna() & df["rt_created_at"].isna() & df["quoted_status_id"].isna()]
    reply = df[df["in_reply_to_user_id"].notna() & df["quoted_status_id"].isna()]
    retweet = df[df["rt_created_at"].notna()]
    quote = df[df["quoted_status_id"].notna() & df["rt_created_at"].isna()]
    return {
        "original": len(original),
        "reply": len(reply),
        "retweet": len(retweet),
        "quote": len(quote)
    }


def print_pie_chart4(title, label, data):
    explode = (0.1, 0.1, 0.1, 0.1)

    # Creating color parameters
    colors = ("lightgreen", "orange", "cyan", "grey")

    # Wedge properties
    wp = {'linewidth': 1, 'edgecolor': "black"}

    # Creating autocpt arguments
    def func(pct, allvalues):
        absolute = int(pct / 100. * np.sum(allvalues))
        return "{:.1f}%\n({:d})".format(pct, absolute)

    # Creating plot
    fig, ax = plt.subplots(figsize=(10, 7))
    wedges, texts, autotexts = ax.pie(data,
                                      autopct=lambda pct: func(pct, data),
                                      explode=explode,
                                      labels=label,
                                      shadow=True,
                                      colors=colors,
                                      startangle=90,
                                      wedgeprops=wp)

    # Adding legend
    ax.legend(wedges, label,
              title="Legend",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1))

    plt.setp(autotexts, size=8, weight="bold")
    ax.set_title(title)
    plt.show()


def hashtag_process_list(lst):
    external_lst = []
    for i in tqdm(lst):
        if i == "[]":
            external_lst.append("[]")
        else:
            internal_lst = []
            for split in i.split("text"):
                if "indices" in split:
                    x = split.split(",")[0][4:-1]
                    internal_lst.append(x)
            external_lst.append(internal_lst)
    return external_lst

def hashtag_extraction(df):
    lst = hashtag_process_list(df["hashtags"])
    lst_2 = [i for i in lst if i != "[]"]
    result = []
    for i in lst_2:
        for j in i:
            result.append(j)
    return result

def extract_domain_list(df):
    value = []
    for i in tqdm(df["urls"]):
        url_exp = i.split(" ")
        lst_inside = []
        for exp in range(len(url_exp)):
            if url_exp[exp] == "'expanded_url':":
                lst_inside.append(url_exp[exp+1][1:-2])
            value.append(lst_inside)
    domain_list = []
    cont = 0
    for url in value:
        cont = cont + 1
        inside = []
        for i in url:
            try:
                x = i.split("/")[2]
            except:
                x = "napolimagazine.com"
            if "www." in x:
                x = x[4:]
            inside.append(x)
        domain_list.append(inside)
    return domain_list

def process_datetime(data):
    if ((data == "nan") or (data == "False") or (data == "None")):
        month = "01"
        day = "01"
        year = "2006"
    else:
        x = data.split()
        month = str(time.strptime(x[1], '%b').tm_mon)
        day = str(x[2])
        year = str(x[5])
    formatted_data = day + "-" + month + "-" + year
    data = str(datetime.datetime.strptime(formatted_data, '%d-%m-%Y')).split()[0]
    return data