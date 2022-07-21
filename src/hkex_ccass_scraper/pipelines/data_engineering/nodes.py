import requests
from bs4 import BeautifulSoup
from lxml import html
import yaml
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import re
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from functools import reduce
import concurrent.futures
import numpy as np

def clean_snake_case(name:str)->str:
    for c in ",;{}()\n\t=":
        name = name.replace(c, "")
    return name.replace(" ", "_").lower()

def transform_participants(df: pd.DataFrame):
    df_new_list = []
    stock_code_list = list(df.stock_code.unique())
    for stock_code in stock_code_list:
        df_sub = df[df.stock_code == stock_code]
        df_sub = df_sub[['participant_id', 'business_date', 'sharepercent']]
        df_sub.set_index(['participant_id','business_date'], inplace=True)
        df_sub['diffs'] = np.nan
        for idx in df_sub.index.levels[0]:
            df_sub.diffs[idx] = df_sub.sharepercent[idx].diff()
        df_sub = df_sub.reset_index()
        df_sub['stock_code'] = ('0000' + str(stock_code))[-5:]
        df_new_list.append(df_sub)
    df_new = pd.concat(df_new_list)
    df_new = df_new.where(df_new.notna(), None)
    sparkse = SparkSession.builder.getOrCreate()
    df = sparkse.createDataFrame(df_new)
    return df

def pandas_for_spark(df: pd.DataFrame, on_column=True, on_row = True) -> DataFrame:
    if isinstance(df, pd.DataFrame):
        if on_row:
            for col in df.columns:
                if np.any([isinstance(val, (frozenset, set, np.ndarray)) for val in df[col]]):
                    print(f"supported conversion for {df[col].dtype} : {col}")
                    df[col] = df[col].apply(lambda a : list(a))
        if on_column:
            df.rename(columns = {col: clean_snake_case(col) for col in df.columns}, inplace = True)
        return df

def get_stock_list(url: str, params: dict, headers: dict, rename_mapper: dict) -> DataFrame:
    sparkse = SparkSession.builder.getOrCreate()
    r_stock_list= requests.get(url, params=params, headers = headers)
    df_stock_list = pd.DataFrame(r_stock_list.json())
    df_stock_list = df_stock_list.rename(rename_mapper, axis='columns')
    df_stock_list = sparkse.createDataFrame(df_stock_list)
    return df_stock_list

def get_stocks_participants_spark(df_stock_list: pd.DataFrame, url: str, data_body: dict, headers: dict, columns: dict, current_date: str, min_date: str, max_date: str) \
    -> DataFrame:
    #df_stock_list = df_stock_list.head(100)
    #df_stock_list = df_stock_list[(df_stock_list.stock_code >= '00600') & (df_stock_list.stock_code <= '00601')]
    df_stock_participants_list= []
    for stock_code in df_stock_list['stock_code']:
        df_stock_participants =  get_stock_participants_spark(url, data_body, headers, columns, current_date, min_date, max_date, stock_code)
        print(f'stock=={stock_code}')
        if df_stock_participants is not None:
            df_stock_participants_list.append(df_stock_participants)
    df_stocks_participants = reduce(DataFrame.unionAll, df_stock_participants_list)
    return df_stocks_participants

def get_stocks_participants(df_stock_list: pd.DataFrame, url: str, data_body: dict, headers: dict, columns: dict, current_date: str, min_date: str, max_date: str) -> pd.DataFrame:
    df_stock_list = df_stock_list.head(2)
    df_stock_participants_list= []
    for stock_code in df_stock_list['stock_code']:
        df_stock_participants =  get_stock_participants(url, data_body, headers, columns, current_date, min_date, max_date, stock_code)
        print(f'stock=={stock_code}')
        if df_stock_participants is not None:
            df_stock_participants_list.append(df_stock_participants)
    df = pd.concat(df_stock_participants_list)
    return df

def get_stocks_participants_spark_concurrent(df_stock_list: pd.DataFrame, \
    url: str, data_body: dict, headers: dict, columns: dict, 
    current_date: str, min_date: str, max_date: str, 
    start: int, end: int) -> DataFrame:
    #df_stock_list = df_stock_list.head(10)
    df_stock_list = df_stock_list[df_stock_list['stock_code'] == '00113']
    #df_stock_list = df_stock_list.iloc[start:end]
    df_stock_participants_list= []
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        stock_code_list = [stock_code for stock_code in df_stock_list['stock_code']]
        future_to_stock_code = {executor.submit(get_stock_participants_spark, url, data_body, headers, columns, current_date, min_date, max_date, stock_code): stock_code for stock_code in stock_code_list}
        for future in concurrent.futures.as_completed(future_to_stock_code):
            try:
                data = future.result()
                if data is not None:
                    df_stock_participants_list.append(data)
            except Exception as exc:
                print('%r generated an exception: %s' % (future_to_stock_code[future], exc))
        print(df_stock_participants_list)
        df = reduce(DataFrame.unionAll, df_stock_participants_list)
    return df

def get_stocks_participants_concurrent(df_stock_list: pd.DataFrame, \
    url: str, data_body: dict, headers: dict, columns: dict, 
    current_date: str, min_date: str, max_date: str, 
    start: int, end: int) -> pd.DataFrame:
    #df_stock_list = df_stock_list.head(100)
    #df_stock_list = df_stock_list[df_stock_list['stock_code'] == '00113']
    df_stock_list = df_stock_list.iloc[start:end]
    df_stock_participants_list= []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        stock_code_list = [stock_code for stock_code in df_stock_list['stock_code']]
        future_to_stock_code = {executor.submit(get_stock_participants, url, data_body, headers, columns, current_date, min_date, max_date, stock_code): stock_code \
            for stock_code in stock_code_list}
        for future in concurrent.futures.as_completed(future_to_stock_code):
            try:
                data = future.result()
                if data is not None:
                    df_stock_participants_list.append(data)
            except Exception as exc:
                print('%r generated an exception: %s' % (future_to_stock_code[future], exc))
    if len(df_stock_participants_list) >= 1:
        df = pd.concat(df_stock_participants_list)
        return df


# def get_stocks_participants(df_stock_list: pd.DataFrame, url: str, data_body: dict, headers: dict, columns: dict, current_date: str, min_date: str, max_date: str) \
#     -> Dict[str, pd.DataFrame]:
#     df_stock_list = df_stock_list.head(2)
#     df_stock_participants_parts = {}
#     for stock_code in df_stock_list['stock_code']:
#         df_stock_participants =  get_stock_participants(url, data_body, headers, columns, current_date, min_date, max_date, stock_code)
#         print(f'stock=={stock_code}')
#         df_stock_participants_parts[f'stock=={stock_code}'] = df_stock_participants
#     return df_stock_participants_parts

def get_stock_participants(url: str, data_body: dict, headers: dict, \
    columns: dict, current_date: str, min_date: str, 
    max_date: str, stock_code: str) -> pd.DataFrame:
    print(f'stock_code in processing is {str(stock_code)}')
    df_response_list = []
    hkex = mcal.get_calendar('HKEX')
    business_date_list = hkex.schedule(start_date=min_date, end_date=max_date).index
    for bd in business_date_list:
      bd_informat = bd.strftime('%Y-%m-%d')
      bd = bd.strftime('%Y/%m/%d')
      data_body['today'] = current_date
      data_body['txtShareholdingDate'] = bd
      data_body['txtStockCode'] = str(stock_code)
      r = requests.post(url, data=data_body, headers = headers)
      soup = BeautifulSoup(r.content, 'html.parser')
      df_response_dict = {}
      for k in columns.keys():
          #print(columns[k])
          tag = columns[k]['tag']
          attr_type = columns[k]['attr_type']
          atrr_value = columns[k]['atrr_value']
          value_class = columns[k]['value_class']
          value_type = columns[k]['value_type']
          col = soup.find_all(tag, attrs={attr_type:atrr_value})
          #print(value_type)
          col_lst = produce_col_list(col, value_class = value_class, value_type = value_type)
          df_response_dict[k] = col_lst
      #df_reponse = pd.DataFrame(df_response_dict)
      df_reponse = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in df_response_dict.items()]))
      df_reponse['business_date'] = str(bd_informat)
      #print(df_reponse)
      df_response_list.append(df_reponse)
    df = pd.concat(df_response_list)
    df['stock_code'] = stock_code
    if df.empty == False:
        df = df.where(df.notna(), None)
    return df

def get_stock_participants_spark(url: str, data_body: dict, headers: dict, columns: dict, current_date: str, min_date: str, max_date: str, stock_code: str) -> DataFrame:
    print(f'stock_code in processing is {str(stock_code)}')
    sparkse = SparkSession.builder.getOrCreate()
    df_response_list = []
    hkex = mcal.get_calendar('HKEX')
    business_date_list = hkex.schedule(start_date=min_date, end_date=max_date).index
    for bd in business_date_list:
        bd_informat = bd.strftime('%Y-%m-%d')
        bd = bd.strftime('%Y/%m/%d')
        data_body['today'] = current_date
        data_body['txtShareholdingDate'] = bd
        data_body['txtStockCode'] = str(stock_code)
        r = requests.post(url, data=data_body, headers = headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        df_response_dict = {}
        for k in columns.keys():
            #print(columns[k])
            tag = columns[k]['tag']
            attr_type = columns[k]['attr_type']
            atrr_value = columns[k]['atrr_value']
            value_class = columns[k]['value_class']
            value_type = columns[k]['value_type']
            col = soup.find_all(tag, attrs={attr_type:atrr_value})
            #print(value_type)
            col_lst = produce_col_list(col, value_class = value_class, value_type = value_type)
            #print(f'business_date is {bd}, column {k} is length {len(col_lst)}')
            df_response_dict[k] = col_lst
        df_reponse_p = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in df_response_dict.items()]))
        df_reponse_p['business_date'] = str(bd_informat)
        df_response_list.append(df_reponse_p)
    df_p = pd.concat(df_response_list)
    #print(df_p.info())
    if df_p.empty == False:
        df_p = df_p.where(df_p.notna(), None)
        df = sparkse.createDataFrame(df_p)
        #'participant_id string, participant_name string, sharenumber long, sharepercent double, business_date string, stock_code string'
        #print(f'stock_code in get_stock_participants_spark is {str(stock_code)}')
        df = df.withColumn('stock_code', F.lit(str(stock_code)))
        print(df.printSchema())
        return df
def produce_col_list(col, value_class, value_type):
    def _parse_value(value, value_type = 'numeric'):
        if value_type == 'numeric':
            return int(re.sub('[,]','',value))
        if value_type == 'percentage':
            return float(re.sub('[%]','',value))/100 
        return str(value)
    return [_parse_value(entry.find('div', attrs = {'class': value_class}).contents[0], value_type) if entry.find('div', attrs = {'class': value_class}).contents else '' for entry in col]
