import pandas as pd
from datetime import datetime

# pd.set_option('display.max_rows', None) # 行
# pd.set_option('display.max_columns', None) # 列

now_date = datetime.now()
now_date = datetime.strftime(now_date, '%Y%m%d')

df = pd.read_csv(f'./data/recently_icook_{now_date}.csv')
columns_name = [
    'product_title',
    'product_brand',
    'product_amount',
    'product_price',
    'product_url',
    'product_spec_detail_text',
]
df.columns = columns_name
df = df.sort_values(by=['product_amount', 'product_price'], ascending=[False, False])
df.to_csv(f'./data/data_sort_icook_{now_date}.csv', mode='w', index=False)
print(df)