import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import tqdm

def main():
    lst = []
    kind = []
    cred = []
    # URL = "https://mediabiasfactcheck.com/leftcenter/"
    # URL = "https://mediabiasfactcheck.com/right-center/"
    # URL = "https://mediabiasfactcheck.com/fake-news/"
    # URL = "https://mediabiasfactcheck.com/conspiracy/"
    URL = "https://mediabiasfactcheck.com/pro-science/"

    print("Start requests:")
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    table = soup.find('tbody')
    # cont = 0
    for i in tqdm.tqdm(table.findAll("td")):
        # cont += 1
        # print(cont)
        # if cont == 23 or cont == 127:
        #     print("Balza")
        if i.a is not None:
            req = requests.get(i.a['href'])
            soup_inside = BeautifulSoup(req.content, "html5lib")
            url = soup_inside.findAll("p")
            for i in url:
                # print(i)
                if "Source: " in i.text:
                    print(i.a["href"])
                    final_url = i.a["href"]
                    lst.append(final_url)
                    cred.append("high")
                    kind.append("pro science")
                    break
    df = pd.DataFrame(list(zip(lst,cred,kind)), columns=["url", "credibility", "description"])
    df.to_csv("pro_science.csv", encoding="utf-8", index=False, line_terminator="\n")
    print("Done!")
if __name__ == '__main__':
    main()
