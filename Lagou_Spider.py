# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 20:22:36 2018

@author: Weizh
"""

import time
from urllib.parse import quote
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import math

#header生成函数
def generate_header(key = '数据分析', city = '全国'):
    header = {'Host': 'www.lagou.com',
              'Origin': 'https://www.lagou.com',
              #https://www.lagou.com/jobs/list_%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90?px=new&city=%E5%85%A8%E5%9B%BD
               'Referer': 'https://www.lagou.com/jobs/list_{}?px=new&city={}'.format(quote(key),quote(city)),
               'Cookie' :'user_trace_token=20180314163431-b3a5992f-95ac-4cdb-84bd-8f86a5e084ad; _ga=GA1.2.521321231.1521016472; LGUID=20180314163432-84e6251a-2762-11e8-b1e3-5254005c3644; index_location_city=%E5%85%A8%E5%9B%BD; JSESSIONID=ABAAABAAAGGABCBD6CF3DF428E3511D25B9B7FB773F7B0E; X_HTTP_TOKEN=b9e62565f65403103924971a79b26efc; LGSID=20180412142155-cc1251f6-3e19-11e8-b747-5254005c3644; LGRID=20180412142622-6b0de44c-3e1a-11e8-ba22-525400f775ce; TG-TRACK-CODE=search_code; SEARCH_ID=231099152a164535b75106a7c3de1c7f',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
               }
    return header

#列表页信息获取函数
def request_url(offset):
    #网页列表页由Ajax异步加载
    #https://www.lagou.com/jobs/positionAjax.json?px=new&city=%E4%B8%8A%E6%B5%B7&needAddtionalResult=false
    url = "https://www.lagou.com/jobs/positionAjax.json"
    payload = {'px': 'new',
               'needAddtionalResult': 'false',
               'city':'全国'
               }
    formdata = {'first': 'false',
                'pn': offset,
                'kd': '数据分析'
                }
    header = generate_header()
    try:
        response = requests.request("POST", url, data=formdata, params=payload, headers=header,timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print("Brief Info Exception: {}".format(e))
        
#提取职位jd函数
def get_descriptions(positionid):
    #根据从列表页获取的positionId爬取jd
    url = 'https://www.lagou.com/jobs/{}.html'.format(positionid)
    header = generate_header()
    response = requests.get(url, headers=header,timeout=10)
    try:
        if response.status_code == 200:
            html_code = response.text
            soup = BeautifulSoup(html_code, 'lxml')
        #jd = job_detail = soup.select('#job_detail > dd.job_bt ',) 
        jd = soup.select('#job_detail > dd.job_bt > div',)                       
        content = jd[0].get_text()
        content_clean = "".join(content.split())
    except Exception as e:
        print("Jd Exception: {}".format(e))
    return content_clean

#职位所有信息提取函数
def getdetail(result): 
    #信息提取
    df = pd.DataFrame()
    data = result['content']['positionResult'] 
    infos = data['result']      
    for info in infos:    
        #列表页信息整理为DataFrame
        df_item = pd.DataFrame.from_dict(info, orient='index').transpose()
        #获取jd
        df_item['content'] = get_descriptions(info['positionId'])
        df =df.append(df_item)  
        time.sleep(3)
    return df

#存取到数据库
def save_sql(df,tablename):
    #打开数据库存储数据
    #dialect[+driver]://user:password@host/dbname[?key=value..]
    engine = create_engine('postgresql://postgres:947172@localhost:5432/postgres') 
  
    try:
        df.to_sql(tablename,engine,index=False,if_exists='append')
        print ('数据存储成功' )  
    except Exception as e:
        print("Sql Exception: {}".format(e))
        
def main(offset):
    positioninfo = getdetail(request_url(offset))
    print('第{}页爬取成功'.format(offset))
    save_sql(positioninfo,"Lagou_DA")
    time.sleep(3)

if __name__ == '__main__':
    try:
        pageCount = math.ceil(request_url(1)['content']['positionResult']['totalCount']/15)
        print('共{0}页'.format(pageCount))
        for page in range(1,pageCount+1):
               main(page)
    except Exception as e:
        print("Page Exception: {}".format(e))
    
       