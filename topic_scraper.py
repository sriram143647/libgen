import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup
session = requests.session()
header = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'
    }

def write_header(): 
    with open('libgen_topics.csv', mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(["ID","Topics","Link"])

def write_data(data):
    with open('libgen_topics.csv', mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def sort_data():
    df = pd.read_csv('libgen_topics.csv')
    df = df.sort_values(by='ID')
    df.to_csv('libgen_topics.csv',index=False)

def get_data():
    write_header()
    page_url = 'https://libgen.rs/'
    res = session.get(page_url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    dataset = soup.find_all('a')
    for data in dataset:
        link_text = data.text
        link = data.get('href').replace('../','https://libgen.rs/')
        if 'topicid' in link:
            id = link.split('topicid')[1].split('&')[0]
            write_data([id,link_text,link])

def start():
    get_data()
    sort_data()

start()