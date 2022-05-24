import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# pd.set_option('display.max_rows', None) # 行
# pd.set_option('display.max_columns', None) # 列

now_date = datetime.now()
diff_date = now_date-relativedelta(days=30)
now_date = datetime.strftime(now_date, '%Y%m%d')
# diff_date = datetime.strftime(diff_date, '%Y%m%d')
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


'''英文欄位'''
# columns_name = [
#     'product_title',
#     'product_brand',
#     'product_amount',
#     'product_price',
#     'product_url',
#     'product_spec',
# ]
# df.columns = columns_name
# df['group_period_start'] = df['group_period_start'].astype('datetime64[ns]')
# df['group_period_end'] = df['group_period_start'].astype('datetime64[ns]')
# df = df[df['group_period_start']>=diff_date]
# # print(df.dtypes)
# df = df.sort_values(by=['product_amount', 'product_price'], ascending=[False, False])
# df.to_csv(f'./data/data_sort_citiesocial_{now_date}.csv', mode='w', index=False)
# print(df)