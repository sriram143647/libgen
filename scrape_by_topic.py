from base64 import encode
import requests
import csv
import json
import pandas as pd
import mysql.connector
import concurrent.futures
from bs4 import BeautifulSoup
session = requests.session()
df = pd.DataFrame()
cols = ["Topic ID","Search page","ID","Authors","File name","File link","File edition","Isin's","Publisher","Publish year","Pages","Language","File size","File extension","Mirror links","Page Url"]

header = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'
    }

def db_insert(df):
    result = df.values.tolist()
    try:
        db_conn = mysql.connector.connect(host='localhost',user='root',database = 'mytestdb2',password='root123')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `my_libgen_db` (`topic_id`,`page`,`id`, `authors`, `file_name`, `file_link`, `file_edition`, `isin`, `publisher`, 
        `publish_year`, `pages`,`lang`,`file_size`,`file_extension`,`mirror_links`,`page_url`) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
        topic_id = VALUES(topic_id), page = VALUES(page), id = VALUES(id), authors = VALUES(authors), file_name = VALUES(file_name),
        file_link = VALUES(file_link), file_edition = VALUES(file_edition) ,isin = VALUES(isin), publisher = VALUES(publisher), 
        publish_year = VALUES(publish_year), pages = VALUES(pages), lang = VALUES(lang), file_size = VALUES(file_size), 
        file_extension = VALUES(file_extension), mirror_links = VALUES(mirror_links), page_url = VALUES(page_url);"""
        cursor.executemany(sql, result)
        db_conn.commit()
        print('data inserted into db')
    except Exception as e:
        print ("Error while connecting to MySQL using Connection pool ", e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print("MySQL connection is closed")

def process_file_data(f_data):
    file_data = list(f_data.stripped_strings)
    if len(file_data) == 1:
        file_name = file_data[0]
        file_link = 'https://libgen.rs/'+f_data.get('href')
        file_ed = None
        file_isin = None
    if len(file_data) == 2:
        file_name = file_data[0]
        file_link = 'https://libgen.rs/'+f_data.get('href')
        file_ed = None
        file_isin = [i.strip() for i in file_data[-1].split(',')]
    if len(file_data) == 3:
        file_name = file_data[0]
        file_link = 'https://libgen.rs/'+f_data.get('href')
        file_ed = file_data[1].replace('\xa0',' ').replace('[',' ').replace(']',' ').strip()
        file_isin = [i.strip() for i in file_data[-1].split(',')]
    isin_dict = {}
    for i in range(5):
        try:
            isin_dict[f'isin{i+1}'] = file_isin[i]
        except:
            pass
    try:
        data = [file_name,file_link,file_ed,isin_dict]
        return data
    except UnboundLocalError:
        return UnboundLocalError

def get_urls(topic_id,param_dict):
    urls = []
    for page in range(1,401):
        page_url = f'https://libgen.rs/search.php?&req=topicid{topic_id}&view='+str(param_dict['view_type'])+'&phrase='+str(param_dict['phrase_bol'])+'&column='+str(param_dict['search_field'])+'&res='+str(param_dict['result_count'])+'&sort='+str(param_dict['sort_column'])+'&open='+str(param_dict['download_type'])+'&sortmode='+str(param_dict['sort_method'])+f'&page={str(page)}'
        urls.append(page_url)
    return urls

def fetch_data(page_url):
    global df
    page = page_url.split('page=')[-1]
    topic_id = page_url.split('topicid')[1].split('&')[0]
    # print(f'scraping topic:{topic_id}, page:{page}')
    print(f'url: {page_url}')
    res = session.get(page_url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    dataset = soup.find_all('table')[2].find_all('tr')
    if len(dataset) == 1:
        return 0
    for data in dataset[1:]:
        info = data.find_all('td')
        if info[6].text == 'English':
            # file id
            id = int(info[0].text)

            # file authors
            auths = [i.strip() for i in info[1].text.split(',')] 
            auth_dict = {}
            for i in range(5):
                try:
                    auth_dict[f'author{i+1}'] = auths[i]
                except:
                    pass

            # file main data 
            f_data = soup.find('a',{'id':id})
            data = process_file_data(f_data)
            
            # file name
            try:
                file_name = data[0]
            except:
                continue
            
            # file link
            file_link = data[1]
            
            # file edition
            file_ed = data[2]
            
            # file isin
            isin_dict = data[3]
            
            # file publisher 
            publisher = info[3].text
            try:
                pub_yr = int(info[4].text)
            except ValueError:
                pub_yr = ''
            
            # file page count
            file_pages = info[5].text
            try:
                if '[' in file_pages and ']' in file_pages:
                    file_pages = int(file_pages.split('[')[1].split(']')[0])
                else:
                    file_pages = int(file_pages)
            except:
                file_pages = ''
            
            # file language
            file_lang = info[6].text
            
            # file size
            file_size = info[7].text.strip()
            
            # file extension
            file_ext = info[8].text
            
            # file mirror links
            mirror_links = [dt.find('a').get('href') for dt in info[9:]]
            mirror_links_dict = {}
            for i in range(len(mirror_links)):
                try:
                    mirror_links_dict[f'mirror_link{i+1}'] = mirror_links[i]
                except:
                    pass
        
            # data append to row
            row = [int(topic_id),int(page),id,json.dumps(auth_dict),file_name,file_link,file_ed,json.dumps(isin_dict),publisher,pub_yr,file_pages,file_lang,file_size,file_ext,json.dumps(mirror_links_dict),page_url]
            df = df.append(pd.DataFrame([row],columns=cols),ignore_index = True)
    
def search_by_topicid(topic_id):
    global df
    params = {
        'view_type':'simple',
        'search_field':'def',
        'phrase_bol':'1',
        'result_count':'50',
        'download_type':'0',
        'sort_method':'ASC',
        'sort_column':'def',
    }
    print(f'topic:{topic_id}')
    urls = get_urls(topic_id,params)
    for url in urls:
        fetch_data(url)
    df.to_csv(f'data_files/libgen_topic_{topic_id}_data.csv',mode='a', encoding='utf-8',index=False)
    df.drop(df.index, inplace=True)
    # df = pd.read_csv('libgen_topic_264_data.csv')
    # db_insert(df)

def start():
    for i in range(100,150):
        search_by_topicid(i)

start()