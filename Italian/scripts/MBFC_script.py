import requests
from bs4 import BeautifulSoup
import pandas as pd
import tqdm


def main():
    lst = []
    cred = []
    lst_urls = ["https://mediabiasfactcheck.com/leftcenter/", "https://mediabiasfactcheck.com/left/",
                "https://mediabiasfactcheck.com/right-center/", "https://mediabiasfactcheck.com/conspiracy/",
                "https://mediabiasfactcheck.com/right/", "https://mediabiasfactcheck.com/fake-news/",
                "https://mediabiasfactcheck.com/pro-science/"]
    for link in lst_urls:
        # cont = 0
        print(f"Start requests link: {link}")
        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html5lib')
        table = soup.find('tbody')
        for i in tqdm.tqdm(table.findAll("td")):
            # cont += 1
            # if cont == 10:
            #     break
            if i.a is not None:
                req = requests.get(i.a['href'])
                soup_inside = BeautifulSoup(req.content, "html5lib")
                paragraphs = soup_inside.findAll("p")
                for i in paragraphs:
                    if "MBFC Credibility Rating: " in i.text:
                        # print(i.text)
                        try:
                            score = i.text.split("MBFC Credibility Rating: ")[1]
                            cred.append(score)
                        except Exception as e:
                            print(e)
                            cred.append("Score not found")
                    if "Source: " in i.text:
                        # print(i.a["href"])
                        try:
                            final_url = i.a["href"]
                            lst.append(final_url)
                        except Exception as e:
                            print(e)
                            lst.append(i.a)
                        break
        print(f"Processed link: {link}")
    df = pd.DataFrame(list(zip(lst, cred)), columns=["url", "credibility"])
    df.to_csv(r"..\script_directory_output\MBFC_output\script_output.csv",
              encoding="utf-8", index=False, line_terminator="\n")


if __name__ == '__main__':
    main()
