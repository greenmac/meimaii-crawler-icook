from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote
import csv
import logging
import os
import pandas as pd
import re
import requests
import sys
import time

now_date = datetime.now()
now_date = datetime.strftime(now_date, '%Y%m%d')

filepath = f'./data/recently_icook_{now_date}.csv'

domain_url = 'https://market.icook.tw/'

def crawlerIcookResultsAll(time_sleep):
    soup = getSoup(domain_url)
    category_link_lists = soup.select('.categories-list__link') # 抓全部商品

    for category_link in category_link_lists:
        category_title = category_link.get_text()
        category_url = domain_url+category_link.get('href')
        category_text = category_title+':'+category_url
        getCategoriesInfo(category_url, time_sleep)

    dataSort()

def crawlerIcookResultsWeekHot(time_sleep):
    soup = getSoup(domain_url)
    category_link_lists = soup.select('.categories-list__link')[0:1] # 抓本週熱銷

    for category_link in category_link_lists:
        category_title = category_link.get_text()
        category_url = domain_url+category_link.get('href')
        category_text = category_title+':'+category_url
        getCategoriesInfo(category_url, time_sleep)
    
    dataSort()
        
def getCategoriesInfo(category_url, time_sleep):
    soup = getSoup(category_url)
    product_link_lists = soup.select('.CategoryProduct-module__categoryProduct___2VsAX')
    
    url_insert_lists = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for product_link in product_link_lists:
            product_title = (
                product_link.select('.CategoryProduct-module__categoryProductName___3Dm_S')[0].get_text())
            product_url = domain_url+product_link.get('href')
            check_exist_lists = checkExistLists()
            if product_url in check_exist_lists:
                print('<'*20)
                print('Url exist:', product_url)
            if product_url not in check_exist_lists:
                print('>'*20)
                print('Url insert:', product_url)
                # getProductsInfo(product_url, time_sleep) # 如果不要異步處理
                executor.submit(getProductsInfo, product_url, time_sleep) # 如果要異步處理
                url_insert_lists.append(product_url)
    print('$'*40)
    print('len_url_insert_lists:', len(url_insert_lists))

def getProductsInfo(product_url, time_sleep):
    soup = getSoup(product_url)
    
    product_title = soup.select('.ProductIntro-module__productIntroTitleName___2wPoJ')
    product_title = product_title[0].get_text() if product_title else ''
    
    product_brand = soup.select('.ProductIntro-module__productIntroBrand___3fscS')
    product_brand = product_brand[0].get_text() if product_brand else ''
    
    product_amount = soup.select('.ProductFundraising-module__productFundraisingIsSellingPriceTotal___2QDNR')
    product_amount = product_amount[0].get_text() if product_amount else ''
    product_amount = int(''.join(re.findall('\d*', product_amount)))  if product_amount else '' # 如果要轉換純數字
        
    product_price = soup.select('.ProductItemsMobile-module__productItemsMobileDetailPrice___3Edqg')
    product_price = product_price[0].get_text().split('NT$ ')[1] if product_price else ''
    product_price = int(''.join(re.findall('\d*', product_price)))  if product_price else '' # 如果要轉換純數字
    
    product_url = product_url

    product_spec_detail = soup.select('p')
    product_spec_lists = ''
    for product_spec in product_spec_detail:
        product_spec_title = product_spec.get_text()
        if '品名' in product_spec_title:     
            product_spec_lists = str(product_spec).split('<br/>')
            product_spec_lists = (
                [i.replace('<p>', '').replace('</p>', '')
                    .replace('<strong>', '').replace('</strong>', '')
                        for i in product_spec_lists]
            )
    product_spec_detail_text = ','.join(product_spec_lists)
    
    product_info_dict = {
        'product_title': product_title,
        'product_brand': product_brand,
        'product_amount': product_amount,
        'product_price': product_price,
        'product_url': product_url,
        'product_spec_detail_text': product_spec_detail_text,
    }
    
    print('*'*20)
    print(product_info_dict)
    df = pd.DataFrame(product_info_dict, index=[0])
    df.to_csv(filepath, mode='a', header=False, index=False)
    time.sleep(time_sleep)
    return product_info_dict

def getSoup(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
    }
    resp = requests.session().get(url, headers=headers)
    resp_results = resp.text 
    soup = BeautifulSoup(resp_results, 'lxml')
    return soup

def checkExistLists():
    check_rows = []
    if os.path.isfile(filepath):
        with open(filepath, 'r', encoding="utf-8", newline='') as csvfile:
            rows = list(csv.reader(csvfile))
            for row in rows:
                check_rows.append(row[4])
    return check_rows

def dataSort():
    # pd.set_option('display.max_rows', None) # 行
    # pd.set_option('display.max_columns', None) # 列

    now_date = datetime.now()
    diff_date = now_date-relativedelta(days=30)
    now_date = datetime.strftime(now_date, '%Y%m%d')
    print(diff_date)

    df = pd.read_csv(f'./data/recently_icook_{now_date}.csv')

    '''中文欄位'''
    columns_name = [
        '商品名稱',
        '品牌名稱',
        '累積金額',
        '產品單價',
        '產品網址',
        '產品規格',
    ]

    df.columns = columns_name
    df = df.loc[df['累積金額'].notnull()]
    df = df.sort_values(by=['累積金額', '產品單價'], ascending=[False, False])
    df.to_csv(f'./data/data_sort_icook_{now_date}.csv', mode='w', index=False)
    print(df)

if __name__ == "__main__":
    start_time = time.time()
    print('start_time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('='*60)

    trigger = sys.argv[1]
    if trigger == 'all': # 全部商品
        crawlerIcookResultsAll(time_sleep=0)
    if trigger == 'week_hot': # 當週熱門
        crawlerIcookResultsWeekHot(time_sleep=0)

    print('='*60)
    end_time = time.time()
    print('end_time:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    cost_time = end_time-start_time
    m, s = divmod(cost_time, 60)
    h, m = divmod(m, 60)
    print(f'cost_time: {int(h)}h:{int(m)}m:{round(s, 2)}s')
