import urllib.request
import numpy as np
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
import argparse
import glob
import os
import selenium
from selenium import webdriver
import time
from PIL import Image
import io
import requests
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import ElementClickInterceptedException


# css-4rbku5 css-18t94o4 css-1dbjc4n r-1loqt21 r-1wbh5a2 r-dnmrzs r-1ny4l3l
class_name = "css-4rbku5 css-18t94o4 css-1dbjc4n r-1loqt21 r-1wbh5a2 r-dnmrzs r-1ny4l3l"
# driver = webdriver.Firefox(GeckoDriverManager().install())
driver = webdriver.Firefox(executable_path=r"C:\Users\gianl\Downloads\gecko\geckodriver.exe")
page = "https://twitter.com/mayemusk/following"
if __name__ == '__main__':
    driver.get(page)
    # Scroll to the end of the page
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)  # sleep_between_interactions
    print(driver.find_element(By.CLASS_NAME, class_name))
