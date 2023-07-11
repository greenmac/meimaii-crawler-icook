from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv
import numpy as np
import os
import pandas as pd
import re
import requests
import time

from utils import timer


now_date = datetime.strftime(datetime.now(), '%Y%m%d')
last_week_date = datetime.date(datetime.now())-relativedelta(days=7)
last_week_date = datetime.strftime(last_week_date, '%Y%m%d')

web_name = 'icook'
file_path = f'./data/recently_{web_name}_{now_date}.csv'

domain_url = 'https://market.icook.tw'

'''全部商品，暫不啟用'''
# @timer
# def crawler_icook_results_all(time_sleep): # 全部商品
#     soup = get_soup(domain_url)
#     category_link_lists = soup.select('.categories-list__link') # 抓全部商品

#     for category_link in category_link_lists:
#         category_title = category_link.get_text()
#         category_url = domain_url+category_link.get('href')
#         category_text = category_title+':'+category_url
#         get_categories_info(category_url, time_sleep)
    
#     get_df_add_header_to_csv()
#     data_sort()
#     amount_limit()

'''本週熱銷排行'''
@timer
def crawler_icook_results_week_hot(time_sleep): # 本週熱銷排行
    soup = get_soup(domain_url)
    category_link_lists = soup.select('.categories-list__link')[0:1] # 抓本週熱銷

    for category_link in category_link_lists:
        category_title = category_link.get_text()
        category_url = domain_url+category_link.get('href')
        category_text = category_title+':'+category_url
        get_categories_info(category_url, time_sleep)
    
    get_df_add_header_to_csv()
    data_sort()
    amount_limit()

def get_categories_info(category_url, time_sleep):
    soup = get_soup(category_url)
    product_link_lists = soup.select('.CategoryProduct-module__categoryProduct___2VsAX')
    
    url_insert_lists = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for product_link in product_link_lists:
            product_title = (
                product_link.select('.CategoryProduct-module__categoryProductName___3Dm_S')[0].get_text())
            product_url = domain_url+product_link.get('href')
            check_exist_lists = get_check_exist_lists()
            if product_url in check_exist_lists:
                print('<'*20)
                print('Url exist:', product_url)
            if product_url not in check_exist_lists:
                print('>'*20)
                print('Url insert:', product_url)
                # get_products_info(product_url, time_sleep) # 如果不要異步處理
                executor.submit(get_products_info, product_url, time_sleep) # 如果要異步處理
                url_insert_lists.append(product_url)
    print('$'*40)
    print('len_url_insert_lists:', len(url_insert_lists))

def get_products_info(product_url, time_sleep):
    soup = get_soup(product_url)
    
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
                    .replace('<strong>', '').replace('</strong>', '').replace('\n', '')
                        for i in product_spec_lists]
            )
    product_spec_detail_text = ','.join(product_spec_lists)
    
    remaining_time = ''
    
    group_period = ''
    
    group_period_start = ''
    
    group_period_end = soup.select('.ProductFundraising-module__productFundraisingDetailInfoDetail___kOo0c')
    group_period_end = group_period_end[1].get_text().replace('截止時間 ', '')+':00' if len(group_period_end)>=2 else ''

    df_file_path = f'./data/recently_{web_name}_{last_week_date}.csv'
    df_last_week_url_list = get_df_last_week_url_list(df_file_path)
    new_product = '✅' if product_url not in df_last_week_url_list else ''
    
    product_info_dict = {
        'product_title': product_title,
        'product_brand': product_brand,
        'product_amount': product_amount,
        'product_price': product_price,
        'product_url': product_url,
        'product_spec_detail_text': product_spec_detail_text,
        'remaining_time': remaining_time,
        'group_period': group_period,
        'group_period_start': group_period_start,
        'group_period_end': group_period_end,
        'new_product': new_product,
    }
    
    print('*'*20)
    print(product_info_dict)
    df = pd.DataFrame(product_info_dict, index=[0])
    df.to_csv(file_path, mode='a', header=False, index=False)
    time.sleep(time_sleep)
    return product_info_dict

def get_soup(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
    }
    resp = requests.session().get(url, headers=headers)
    resp_results = resp.text 
    soup = BeautifulSoup(resp_results, 'lxml')
    return soup

def get_check_exist_lists():
    check_rows = []
    if os.path.isfile(file_path):
        with open(file_path, 'r', encoding="utf-8", newline='') as csvfile:
            rows = list(csv.reader(csvfile))
            for row in rows:
                check_rows.append(row[4])
    return check_rows

def get_df_last_week_url_list(df_file_path):
    df_last_week = pd.read_csv(df_file_path)
    df_last_week_url_list = df_last_week.values.tolist()
    df_last_week_url_list = [i[4] for i in df_last_week_url_list]
    return df_last_week_url_list

def get_df_add_header_to_csv():
    columns_name = [
        'product_title',
        'product_brand',
        'product_amount',
        'product_price',
        'product_url',
        'product_spec_detail_text',
        'remaining_time',
        'group_period',
        'group_period_start',
        'group_period_end',
        'new_product',
    ]
    df = pd.read_csv(file_path, header=None)
    df_temp = df[0:1][0].values
    if 'product_title' in df_temp:
        df = pd.read_csv(file_path)
    if 'product_title' not in df_temp:
        df.columns = columns_name
    df.to_csv(file_path, index=False)

def data_sort():
    now_date = datetime.now()
    diff_date = now_date-relativedelta(days=30)
    now_date = datetime.strftime(now_date, '%Y%m%d')

    df = pd.read_csv(file_path)

    '''中文欄位'''
    columns_name = [
        '商品名稱',
        '品牌名稱',
        '累積金額',
        '商品單價',
        '商品網址',
        '商品規格',
        '剩餘時間',
        '集資期間(30天內開團的商品)',
        '集資開始',
        '集資結束',
        '新品入榜',
    ]

    df.columns = columns_name
    df = df.loc[df['累積金額'].notnull()]
    limit_amount = 500000 # 限制多少金額才列出
    df = df[df['累積金額']>=limit_amount]

    df_file_path = f'./data/data_sort_{web_name}_{last_week_date}.csv'
    df_last_week_url_list = get_df_last_week_url_list(df_file_path)
    df['新品入榜'] = np.where(~df['商品網址'].isin(df_last_week_url_list)==True, '✅', '')

    df = df.sort_values(by=['新品入榜', '累積金額'], ascending=[False, False])
    df.to_csv(f'./data/data_sort_{web_name}_{now_date}.csv', mode='w', index=False)

def amount_limit():
    now_date = datetime.now()
    now_date = datetime.strftime(now_date, '%Y%m%d')

    df = pd.read_csv(file_path)

    '''中文欄位'''
    columns_name = [
        '商品名稱',
        '品牌名稱',
        '累積金額',
        '商品單價',
        '商品網址',
        '商品規格',
        '剩餘時間',
        f'集資期間({now_date} 截止)',
        '集資開始',
        '集資結束',
        '新品入榜',
    ]

    df.columns = columns_name
    limit_amount = 5000000 # 限制多少金額才列出
    df = df[df['累積金額']>=limit_amount]
    
    df_file_path = f'./data/amount_limit_{web_name}_{last_week_date}.csv'
    df_last_week_url_list = get_df_last_week_url_list(df_file_path)
    df['新品入榜'] = np.where(~df['商品網址'].isin(df_last_week_url_list)==True, '✅', '')

    df = df.sort_values(by=['新品入榜', '累積金額'], ascending=[False, False])
    df.to_csv(f'./data/amount_limit_{web_name}_{now_date}.csv', mode='w', index=False)

if __name__ == "__main__":
    # crawler_icook_results_all(time_sleep=0) # 全部商品
    crawler_icook_results_week_hot(time_sleep=0) # 本週熱銷排行
