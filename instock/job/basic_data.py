#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import json
import math
import requests
import numpy as np
import pandas as pd
import time
import datetime 
import mysql.connector
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import pandas_market_calendars as mcal
from mysql.connector import Error
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 




numeric_cols = ["f2", "f3", "f4", "f5", "f6", "f7", "f8", "f10", "f15", "f16", "f17", "f18", "f22", "f11", "f24", "f25", "f9", "f115", "f114", "f23", "f112", "f113", "f61", "f48", "f37", "f49", "f57", "f40", "f41", "f45", "f46", "f38", "f39", "f20", "f21" ]
date_cols = ["f26", "f221"]

def is_open(price):
    return not np.isnan(price)

"""
获取沪市A股+深市A股所有股票指标数据
1、获取今日股票的每日数据，需要在闭市后运行才能获取的date今日数据，开始前获取的date是昨日数据(tablestructure中TABLE_CN_STOCK_SELECTION定义了MAX_TRADE_DATE作为date)
"""


########################################
#获取沪市A股+深市A股所有股票指标数据并写入数据库

def stock_selection() -> pd.DataFrame:
    '''
    东方财富网-个股-选股器
    https://data.eastmoney.com/xuangu/
    :return: 选股器
    :rtype: pandas.DataFrame
    获取沪市A股+深市A股所有股票数据
    api1：https://datacenter-web.eastmoney.com/wstock/selection/api/data/get?type=RPTA_PCNEW_STOCKSELECT&sty=SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE&filter=(MARKET+in+("上交所主板","深交所主板","深交所创业板","上交所科创板","上交所风险警示板","深交所风险警示板"))&p=1&ps=50&st=CHANGE_RATE&sr=-1&source=SELECT_SECURITIES&client=WEB
    api2：https://data.eastmoney.com/dataapi/xuangu/list?st=CHANGE_RATE&sr=-1&ps=50&p=1&sty=SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE&filter=(MARKET+in+("上交所主板","上交所风险警示板","上交所科创板","上交所主板","深交所主板","深交所创业板"))&source=SELECT_SECURITIES&client=WEB 
    注意：https://datacenter-web.eastmoney.com/返回的数据没有股票总数字段
    '''
    table_name = tbs.TABLE_CN_STOCK_SELECTION['name']
    cols = tbs.TABLE_CN_STOCK_SELECTION['columns']
    page_size = 1000

    # 通过sty获取需要的哪些股票数据，初始值： "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE"
    # 生成字段字符串
    sty = ','.join(
        cols[k]['map'] 
        for k in cols 
        if cols[k]['map']
    )

    # map生成英文映射字典
    en_name = {
        cols[k]['map']: cols[k]['en']
        for k in cols
        if 'map' in cols[k] and cols[k]['map']
    }

    exclude_columns = ['code', 'name', 'date'] # 定义列名，即字段名
    
    # 生成映射字典
    cn_name = {
        cols[k]['map']: cols[k]['cn']
        for k in cols
        if cols[k]['map'] and k not in exclude_columns
    }

    # print(f'{cn_name}')

    url = "https://data.eastmoney.com/dataapi/xuangu/list"
    params = {
        "sty": sty,
        "filter": "(MARKET+in+(\"上交所主板\",\"深交所主板\",\"深交所创业板\",\"上交所科创板\",\"上交所风险警示板\",\"深交所风险警示板\"))",
        "p": "1",
        "ps": page_size,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }
    try:

        # 1. 获取数据并预处理
        temp_df = fetch_selection_data(url,params,page_size,"p").replace({np.nan: None}) # 将数据中NaN空数据进行替换：替换np.nan为None
        # print(f'{temp_df}')

        mask = ~temp_df['CONCEPT'].isna()
        temp_df.loc[mask, 'CONCEPT'] = temp_df.loc[mask, 'CONCEPT'].apply(lambda x:', '.join(x))
        mask = ~temp_df['STYLE'].isna()
        temp_df.loc[mask, 'STYLE'] = temp_df.loc[mask, 'STYLE'].apply(lambda x: ', '.join(x))
        temp_df.rename(columns=en_name, inplace=True) # 将默认列名改为英文列名
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')
        print(f'{temp_df}')

        # ==== 主表（cn_stock_selection）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(table_name)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, table_name, temp_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
          # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=table_name,
                    data=temp_df,
                    batch_size=1000  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 自选主表写入完成，数据量：{len(temp_df)}")
            except Exception as e:
                print(f"[Critical] 自选主表批量写入失败: {str(e)}")
            
        except Exception as e:
            print(f"[Error] 自选主表写入失败: {e}")
        #################################################

        return temp_df
    except Exception as e:
        print(f"东方财富网-获取沪市A股+深市A股所有股票数据 {temp_df} 时出错: {e}")

#获取沪市A股+深市A股，接口请求数据
def fetch_selection_data(
    url: str,
    params: Dict,
    page_size: int,
    page_param_name: str = "p",  # 分页参数名，默认为 "p"
    start_page: int = 1          # 起始页码，默认为 1
    ) -> pd.DataFrame:
     '''
     多线程获取[选股器]所有分页数据并返回DataFrame

     Args:
         url: 请求地址
         params: 基础请求参数(不要包含页码参数p)
         page_size: 每页大小

     Returns:
         合并后的DataFrame
     '''
     # 1. 获取第一页数据并计算总页数
     first_page_params = {**params, page_param_name: start_page}
     try:
         r = requests.get(url, params=first_page_params)
         r.raise_for_status()
         data_json = r.json()
         data_count = data_json["result"]["count"]
         page_size = len(data_json["result"]["data"])
         page_total = math.ceil(data_count / page_size)
         data = data_json["result"]["data"]
     except Exception as e:
         print(f"初始页获取失败: {str(e)}")
         return pd.DataFrame()

     # 2. 多线程获取剩余页面
     def fetch_page(page: int) -> List[Dict]:
         page_params = {**params, page_param_name: page}
         try:
             r = requests.get(url, params=page_params)
             r.raise_for_status()
             return r.json()["result"]["data"]
         except Exception:
             return []

     with ThreadPoolExecutor() as executor:
         # 从第2页开始提交任务
         futures = {
             executor.submit(fetch_page, page): page
             for page in range(2, page_total + 1)
         }

         # 进度条显示
         for future in tqdm(as_completed(futures),
                            total=len(futures),
                            desc="并发拉取数据"):
             page_data = future.result()
             if page_data:
                 data.extend(page_data)

     return pd.DataFrame(data)


########################################
#获取沪市A股+深市A股实时股票数据数据并写入数据库

def stock_zh_a_spot_em() -> pd.DataFrame:
    '''
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    '''
    #实时行情数据主表
    table_name = tbs.TABLE_CN_STOCK_SPOT['name']
    table_name_cols = tbs.TABLE_CN_STOCK_SPOT['columns']

    # 生成字段字符串，确保cols[k]中存在'map'键，并且其值不为空。
    fields = ','.join(
        table_name_cols[k]['map']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    )
    
    # map生成中文映射字典
    cn_name = {
        table_name_cols[k]['map']: table_name_cols[k]['cn']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }
    
    # map生成英文映射字典
    en_name = {
        table_name_cols[k]['map']: table_name_cols[k]['en']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }

    page_size = 1000
    page_current = 1
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": '1000',
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        # "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "fields": fields,
        "_": "1623833739532",
    }
    try:
        temp_df = fetch_zh_a_spot_data(url,params,page_size,"pn")# 将数据中NaN空数据进行替换：替换np.nan为None

        # 定义数值列清单
        numeric_cols = [
            'f2','f3','f4','f5','f6','f7','f8','f9','f10','f11',
            'f15','f16','f17','f18','f20','f21','f22','f23','f24','f25',
            'f37','f38','f39','f40','f41','f45','f46','f48','f49','f57','f61',
            'f112','f113','f114','f115'
        ]
                
        # 执行类型转换
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        date_cols = ["f26", "f221"]
        temp_df[date_cols] = temp_df[date_cols].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce"))         
        temp_df.rename(columns=en_name, inplace=True) # 将默认列名改为英文列名

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        temp_df.loc[:, "date"] = latest_trade_date
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

        temp_df = temp_df.loc[temp_df['new_price'].apply(is_open)]
        temp_df = temp_df.replace({np.nan: None}) 

        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        # print(f'实时行情数据主表{temp_df}')


        # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(table_name)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, table_name, temp_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=table_name,
                    data=temp_df,
                    batch_size=1000  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 股票行情主表批量写入完成，数据量：{len(temp_df)}")
            except Exception as e:
                print(f"[Critical] 股票行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 股票主表写入失败: {e}")
        #################################################


        #在temp_df只获取stock_info_cols = tbs.TABLE_STOCK_INIT['columns']中配置的字段生成股票基础表

        #股票基础数据表
        stock_info = tbs.TABLE_STOCK_INIT['name'] # cn_stock_info
        stock_info_cols = tbs.TABLE_STOCK_INIT['columns']

        
        # ==== 基础信息表（cn_stock_info）写入逻辑 ====
        try:
            # 1. 筛选需要的列
            required_columns = [col_info['en'] for col_info in stock_info_cols.values() if 'en' in col_info]
            existing_columns = [col for col in required_columns if col in temp_df.columns]
            if not existing_columns:
                print("[Error] 无有效列可写入基础表")
                return pd.DataFrame()

            stock_info_df = temp_df[existing_columns]

            # 手动设置固定日期（例如 1212-12-12）做索引使用
            fixed_date = "2222-12-22"
            # fixed_date = latest_trade_date
            stock_info_df = temp_df[existing_columns].copy()  # 显式创建独立副本
            stock_info_df.loc[:, "date"] = fixed_date
            stock_info_df.loc[:, "date_int"] = stock_info_df["date"].astype(str).str.replace('-', '')
            if "market_id" in stock_info_df.columns and "code" in stock_info_df.columns:
                stock_info_df.loc[:, "code_market"] = (
                    stock_info_df["market_id"].astype(str) + "." +
                    stock_info_df["code"].astype(str).str.zfill(6)
                )
            # stock_info_df.loc[:, "industry"] = fixed_date
            print(f"[Success] stock_info_df：{stock_info_df}")
            print(f"[Success] stock_info_df-columns：{stock_info_df.columns}")

            
            # 2. 创建表（如果不存在）
            create_table_if_not_exists(stock_info)
            
            # 3. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, stock_info, stock_info_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 4. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=stock_info,
                    data=stock_info_df,
                    batch_size=1000  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                
                print(f"[Success] 股票基础信息表写入完成，数据量：{len(stock_info_df)}")
            except Exception as e:
                print(f"[Critical] 股票基础表批量写入失败: {str(e)}")

            return temp_df
        except Exception as e:
            print(f"[Error] 股票写入失败: {e}")
    except Exception as e:
        print(f"东方财富网-沪深京 A 股-实时行情处理失败: {e}")
        return pd.DataFrame()



#获取沪市A股+深市A股实时股票数据，接口请求数据
def fetch_zh_a_spot_data(
    url: str,
    params: Dict,
    page_size: int,
    page_param_name: str = "pn",  # 分页参数名应为页码参数（pn）
    start_page: int = 1           # 起始页码默认为1
    ) -> pd.DataFrame:

    first_page_params = {
        **params,
        "pz": page_size,          # 固定每页大小参数为pz
        page_param_name: start_page  # 起始页码
    }
    try:
        r = requests.get(url, params=first_page_params)
        r.raise_for_status()
        data_json = r.json()
        data_count = data_json["data"]["total"]
        page_size = len(data_json["data"]["diff"])
        page_total = math.ceil(data_count / page_size)
        data = data_json["data"]["diff"]
        print(f"[Debug] 总数据量: {data_count}, 总页数: {page_total}")
    except Exception as e:
        print(f"初始页获取失败: {str(e)}")
        return pd.DataFrame()

    # 2. 多线程请求剩余页
    def fetch_page(page: int) -> List[Dict]:
        page_params = {
            **params,
            "pz": page_size,       # 固定每页大小
            page_param_name: page   # 正确设置页码参数
        }
        try:
            r = requests.get(url, params=page_params)
            r.raise_for_status()
            return r.json()["data"]["diff"]
        except Exception as e:
            print(f"第{page}页请求失败: {e}")
            return []

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_page, page): page
            for page in range(start_page + 1, page_total + 1)  # 从第二页开始
        }

        # 进度条显示
        for future in tqdm(as_completed(futures), total=len(futures), desc="并发拉取数据"):
            page_data = future.result()
            if page_data:
                data.extend(page_data)

    return pd.DataFrame(data)


########################################
#获取ETF基金实时数据并写入数据库

def etf_spot_em() -> pd.DataFrame:
    """
    东方财富-ETF 实时行情
    https://quote.eastmoney.com/center/gridlist.html#fund_etf
    :return: ETF 实时行情
    :rtype: pandas.DataFrame
    """
    #实时行情数据主表
    table_name = tbs.TABLE_CN_ETF_SPOT['name'] # cn_etf_spot
    table_name_cols = tbs.TABLE_CN_ETF_SPOT['columns']

    # 生成字段字符串
    fields = ','.join(
        table_name_cols[k]['map']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    )

    # map生成中文映射字典
    cn_name = {
        table_name_cols[k]['map']: table_name_cols[k]['cn']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }
    
    # map生成英文映射字典
    en_name = {
        table_name_cols[k]['map']: table_name_cols[k]['en']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }

    page_size = 1000
    page_current = 1

    url = "http://88.push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": "1000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f3",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        "fields": fields,
        # "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": "1672806290972",
    }
    try:
        # temp_df = etf_spot_data(url,params,page_size,"pn").replace({np.nan: None}) # 将数据中NaN空数据进行替换：替换np.nan为None
        temp_df = etf_spot_data(url,params,page_size,"pn")
        # ==== 智能数值列转换 ====
        numeric_cols = [
            'f2','f3','f4','f5','f6','f7','f8','f9','f10','f11',
            'f15','f16','f17','f18','f20','f21','f22','f23','f24','f25',
            'f37','f38','f39','f40','f41','f45','f46','f48','f49','f57','f61',
            'f112','f113','f114','f115'
        ]
        
        valid_numeric_cols = [col for col in numeric_cols if col in temp_df.columns]
        
        if valid_numeric_cols:
            temp_df[valid_numeric_cols] = temp_df[valid_numeric_cols].apply(
                pd.to_numeric, errors='coerce'
            )
            print(f"成功转换数值列：{valid_numeric_cols}")
        else:
            print("警告：未找到任何可转换的数值列")

        # ==== 智能日期列转换 ====

        date_cols = ["f26", "f221"]

        valid_date_cols = [col for col in date_cols if col in temp_df.columns]
        
        if valid_date_cols:
            temp_df[valid_date_cols] = temp_df[valid_date_cols].apply(
                lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce")
            )
            print(f"成功转换日期列：{valid_date_cols}")
        else:
            print("警告：未找到任何可转换的日期列")

        #################################################################
        # temp_df[date_cols] = temp_df[date_cols].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce"))         
        temp_df.rename(columns=en_name, inplace=True) # 将默认列名改为英文列名

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2022-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        # print(f"最后的交易日期：{latest_trade_date}")
        temp_df.loc[:, "date"] = latest_trade_date
        # print(f'实时ETF基金数据主表{temp_df}')
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

        temp_df = temp_df.loc[temp_df['new_price'].apply(is_open)]
        temp_df = temp_df.replace({np.nan: None}) 

         # ==== 主表（cn_etf_spot）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(table_name)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, table_name, temp_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=table_name,
                    data=temp_df,
                    batch_size=500  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                
                print(f"[Success] ETF实时行主表批量写入完成，数据量：{len(temp_df)}")
            except Exception as e:
                print(f"[Critical] ETF实时行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Critical] ETF批量写入失败: {str(e)}")
        #################################################



        #在temp_df只获取stock_info_cols = tbs.TABLE_STOCK_INIT['columns']中配置的字段生成股票基础表
        #股票基础数据表
        etf_info = tbs.TABLE_ETF_INIT['name'] # cn_etf_info
        etf_info_cols = tbs.TABLE_ETF_INIT['columns']

        # 1. 筛选需要的列
        required_columns = [col_info['en'] for col_info in etf_info_cols.values() if 'en' in col_info]
        existing_columns = [col for col in required_columns if col in temp_df.columns]
        if not existing_columns:
            print("[Error] 无有效列可写入基础表")
            return pd.DataFrame()
        etf_info_df = temp_df[existing_columns]


        # 手动设置固定日期（例如 1212-12-12）做索引使用
        fixed_date = "2222-12-22"
        # fixed_date = latest_trade_date
        etf_info_df = temp_df[existing_columns].copy()  # 显式创建独立副本
        etf_info_df.loc[:, "date"] = fixed_date  # 使用 .loc 进行安全赋值
        etf_info_df.loc[:, "date_int"] = etf_info_df["date"].astype(str).str.replace('-', '')
        if "market_id" in etf_info_df.columns and "code" in etf_info_df.columns:
            etf_info_df.loc[:, "code_market"] = (
                etf_info_df["market_id"].astype(str) + "." +
                etf_info_df["code"].astype(str).str.zfill(6)
            )
        print(f"[Success] etf_info_df：{etf_info_df}")

         # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(etf_info)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, etf_info, etf_info_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=etf_info,
                    data=etf_info_df,
                    batch_size=500  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                
                print(f"[Success] 基金基础表批量写入完成，数据量：{len(etf_info_df)}")
            except Exception as e:
                print(f"[Critical] 基金基础表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 基金基础表写入失败: {e}")
        ################################################

    except Exception as e:
        print(f"东方财富网-沪深京 A 股-基金行情处理失败: {e}")
        return pd.DataFrame()

#获取ETF基金实时数据，接口请求数据
def etf_spot_data(
    url: str,
    params: Dict,
    page_size: int,
    page_param_name: str = "pn",  # 分页参数名应为页码参数（pn）
    start_page: int = 1           # 起始页码默认为1
    ) -> pd.DataFrame:

    first_page_params = {
        **params,
        "pz": page_size,          # 固定每页大小参数为pz
        page_param_name: start_page  # 起始页码
    }
    try:
        r = requests.get(url, params=first_page_params)
        r.raise_for_status()
        data_json = r.json()
        data_count = data_json["data"]["total"]
        page_size = len(data_json["data"]["diff"])
        page_total = math.ceil(data_count / page_size)
        data = data_json["data"]["diff"]
        print(f"[Debug] 总数据量: {data_count}, 总页数: {page_total}")
    except Exception as e:
        print(f"初始页获取失败: {str(e)}")
        return pd.DataFrame()

    # 2. 多线程请求剩余页
    def fetch_page(page: int) -> List[Dict]:
        page_params = {
            **params,
            "pz": page_size,       # 固定每页大小
            page_param_name: page   # 正确设置页码参数
        }
        try:
            r = requests.get(url, params=page_params)
            r.raise_for_status()
            return r.json()["data"]["diff"]
        except Exception as e:
            print(f"第{page}页请求失败: {e}")
            return []

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_page, page): page
            for page in range(start_page + 1, page_total + 1)  # 从第二页开始
        }

        # 进度条显示
        for future in tqdm(as_completed(futures), total=len(futures), desc="并发拉取数据"):
            page_data = future.result()
            if page_data:
                data.extend(page_data)

    return pd.DataFrame(data)



########################################
#获取国内各大指数实时数据并写入数据库

def index_zh_a_spot_em() -> pd.DataFrame:

    #实时行情数据主表
    table_name = tbs.TABLE_CN_INDEX_SPOT['name'] # cn_index_spot
    table_name_cols = tbs.TABLE_CN_INDEX_SPOT['columns']

    # 通过sty获取需要的哪些股票数据，初始值： "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE"
    # 生成字段字符串
    fields = ','.join(
        table_name_cols[k]['map']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    )

    # map生成中文映射字典
    cn_name = {
        table_name_cols[k]['map']: table_name_cols[k]['cn']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }
    
    # map生成英文映射字典
    en_name = {
        table_name_cols[k]['map']: table_name_cols[k]['en']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }

    page_size = 1000
    page_current = 1

    url = "http://push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "b:MK0010",
        "fields": fields,
        # "fields": "f12,f14,f2,f4,f3,f5,f6,f18,f17,f15,f16",
        "_": "1623833739532",
    }
    try:
        # temp_df = index_spot_data(url,params,page_size,"pn").replace({np.nan: None}) # 将数据中NaN空数据进行替换：替换np.nan为None
        temp_df = index_spot_data(url,params,page_size,"pn")
        # ==== 智能数值列转换 ====
        numeric_cols = [
            'f2','f3','f4','f5','f6','f7','f8','f9','f10','f11',
            'f15','f16','f17','f18','f20','f21','f22','f23','f24','f25',
            'f37','f38','f39','f40','f41','f45','f46','f48','f49','f57','f61',
            'f112','f113','f114','f115'
        ]
        
        valid_numeric_cols = [col for col in numeric_cols if col in temp_df.columns]
        
        if valid_numeric_cols:
            temp_df[valid_numeric_cols] = temp_df[valid_numeric_cols].apply(
                pd.to_numeric, errors='coerce'
            )
            print(f"成功转换数值列：{valid_numeric_cols}")
        else:
            print("警告：未找到任何可转换的数值列")

        # ==== 智能日期列转换 ====

        date_cols = ["f26", "f221"]

        valid_date_cols = [col for col in date_cols if col in temp_df.columns]
        
        if valid_date_cols:
            temp_df[valid_date_cols] = temp_df[valid_date_cols].apply(
                lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce")
            )
            print(f"成功转换日期列：{valid_date_cols}")
        else:
            print("警告：未找到任何可转换的日期列")

        #################################################################
        # print(f'实时指数数据主表{temp_df}')
        # temp_df[date_cols] = temp_df[date_cols].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce"))         
        temp_df.rename(columns=en_name, inplace=True) # 将默认列名改为英文列名

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        # print(f"最后的交易日期：{latest_trade_date}")
        temp_df.loc[:, "date"] = latest_trade_date
        # print(f'实时指数数据主表{temp_df}')
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

        temp_df = temp_df.loc[temp_df['new_price'].apply(is_open)]
        temp_df = temp_df.replace({np.nan: None}) 
         # ==== 主表（cn_etf_spot）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(table_name)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, table_name, temp_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=table_name,
                    data=temp_df,
                    batch_size=500  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 实时指数主表批量写入完成，数据量：{len(temp_df)}")
            except Exception as e:
                print(f"[Critical] 实时指数主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 指数主表写入失败: {e}")
        #################################################



        #在temp_df只获取stock_info_cols = tbs.TABLE_STOCK_INIT['columns']中配置的字段生成股票基础表
        #股票基础数据表
        index_info = tbs.TABLE_INDEX_INIT['name'] # cn_etf_info
        index_info_cols = tbs.TABLE_INDEX_INIT['columns']

        # 1. 筛选需要的列
        required_columns = [col_info['en'] for col_info in index_info_cols.values() if 'en' in col_info]
        existing_columns = [col for col in required_columns if col in temp_df.columns]
        if not existing_columns:
            print("[Error] 无有效列可写入基础表")
            return pd.DataFrame()
        index_info_df = temp_df[existing_columns]


        # 手动设置固定日期（例如 1212-12-12）做索引使用
        fixed_date = "2222-12-22"
        # fixed_date = latest_trade_date
        index_info_df = temp_df[existing_columns].copy()  # 显式创建独立副本
        index_info_df.loc[:, "date"] = fixed_date  # 使用 .loc 进行安全赋值
        index_info_df.loc[:, "date_int"] = index_info_df["date"].astype(str).str.replace('-', '')
        if "market_id" in index_info_df.columns and "code" in index_info_df.columns:
            index_info_df.loc[:, "code_market"] = (
                index_info_df["market_id"].astype(str) + "." +
                index_info_df["code"].astype(str).str.zfill(6)
            )
        print(f"[Success] index_info_df：{index_info_df}")

         # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(index_info)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, index_info, index_info_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=index_info,
                    data=index_info_df,
                    batch_size=500  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 指数基础信息表写入完成，数据量：{len(index_info_df)}")
            except Exception as e:
                print(f"[Critical] 指数基础信息表批量写入失败: {str(e)}")
            
        except Exception as e:
            print(f"[Error] 指数基础信息表写入失败: {e}")
        #################################################

 
        # # 批量转换数据类型
        # numeric_cols = [
        #     "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "最高", "最低", "今开", "昨收"]
        # temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        # print(f"temp_df: {temp_df}")

    except requests.RequestException as e:
        print(f"index_zh_a_spot_em 获取国内各大指数实时数据 请求出错: {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"index_zh_a_spot_em 获取国内各大指数实时数据 解析数据出错: {e}")
        return pd.DataFrame()

#获取ETF基金实时数据，接口请求数据
def index_spot_data(
    url: str,
    params: Dict,
    page_size: int,
    page_param_name: str = "pn",  # 分页参数名应为页码参数（pn）
    start_page: int = 1           # 起始页码默认为1
    ) -> pd.DataFrame:

    first_page_params = {
        **params,
        "pz": page_size,          # 固定每页大小参数为pz
        page_param_name: start_page  # 起始页码
    }
    try:
        r = requests.get(url, params=first_page_params)
        r.raise_for_status()
        data_json = r.json()
        data_count = data_json["data"]["total"]
        page_size = len(data_json["data"]["diff"])
        page_total = math.ceil(data_count / page_size)
        data = data_json["data"]["diff"]
        print(f"[Debug] 总数据量: {data_count}, 总页数: {page_total}")
    except Exception as e:
        print(f"初始页获取失败: {str(e)}")
        return pd.DataFrame()

    # 2. 多线程请求剩余页
    def fetch_page(page: int) -> List[Dict]:
        page_params = {
            **params,
            "pz": page_size,       # 固定每页大小
            page_param_name: page   # 正确设置页码参数
        }
        try:
            r = requests.get(url, params=page_params)
            r.raise_for_status()
            return r.json()["data"]["diff"]
        except Exception as e:
            print(f"第{page}页请求失败: {e}")
            return []

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_page, page): page
            for page in range(start_page + 1, page_total + 1)  # 从第二页开始
        }

        # 进度条显示
        for future in tqdm(as_completed(futures), total=len(futures), desc="并发拉取数据"):
            page_data = future.result()
            if page_data:
                data.extend(page_data)

    return pd.DataFrame(data)




########################################
#获取行业实时股票数据数据并写入数据库

def stock_zh_a_spot_em() -> pd.DataFrame:
    '''
    东方财富网-沪深京 A 股-行业实时行情
    https://quote.eastmoney.com/center/gridlist.html#industry_board
    :return: 行业实时行情
    :rtype: pandas.DataFrame
    '''
    #实时行情数据主表
    table_name = tbs.TABLE_CN_STOCK_SPOT['name']
    table_name_cols = tbs.TABLE_CN_STOCK_SPOT['columns']

    # 生成字段字符串，确保cols[k]中存在'map'键，并且其值不为空。
    fields = ','.join(
        table_name_cols[k]['map']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    )
    
    # map生成中文映射字典
    cn_name = {
        table_name_cols[k]['map']: table_name_cols[k]['cn']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }
    
    # map生成英文映射字典
    en_name = {
        table_name_cols[k]['map']: table_name_cols[k]['en']
        for k in table_name_cols
        if 'map' in table_name_cols[k] and table_name_cols[k]['map']
    }

    page_size = 1000
    page_current = 1
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": '1000',
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        # "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "fields": fields,
        "_": "1623833739532",
    }
    try:
        temp_df = fetch_zh_a_spot_data(url,params,page_size,"pn")# 将数据中NaN空数据进行替换：替换np.nan为None

        # 定义数值列清单
        numeric_cols = [
            'f2','f3','f4','f5','f6','f7','f8','f9','f10','f11',
            'f15','f16','f17','f18','f20','f21','f22','f23','f24','f25',
            'f37','f38','f39','f40','f41','f45','f46','f48','f49','f57','f61',
            'f112','f113','f114','f115'
        ]
                
        # 执行类型转换
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        date_cols = ["f26", "f221"]
        temp_df[date_cols] = temp_df[date_cols].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce"))         
        temp_df.rename(columns=en_name, inplace=True) # 将默认列名改为英文列名

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        temp_df.loc[:, "date"] = latest_trade_date
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

        temp_df = temp_df.loc[temp_df['new_price'].apply(is_open)]
        temp_df = temp_df.replace({np.nan: None}) 

        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        # print(f'实时行情数据主表{temp_df}')


        # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            # 1. 创建表（如果不存在）
            create_table_if_not_exists(table_name)
            
            # 2. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, table_name, temp_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=table_name,
                    data=temp_df,
                    batch_size=1000  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 股票行情主表批量写入完成，数据量：{len(temp_df)}")
            except Exception as e:
                print(f"[Critical] 股票行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 股票主表写入失败: {e}")
        #################################################


        #在temp_df只获取stock_info_cols = tbs.TABLE_STOCK_INIT['columns']中配置的字段生成股票基础表

        #股票基础数据表
        stock_info = tbs.TABLE_STOCK_INIT['name'] # cn_stock_info
        stock_info_cols = tbs.TABLE_STOCK_INIT['columns']

        
        # ==== 基础信息表（cn_stock_info）写入逻辑 ====
        try:
            # 1. 筛选需要的列
            required_columns = [col_info['en'] for col_info in stock_info_cols.values() if 'en' in col_info]
            existing_columns = [col for col in required_columns if col in temp_df.columns]
            if not existing_columns:
                print("[Error] 无有效列可写入基础表")
                return pd.DataFrame()

            stock_info_df = temp_df[existing_columns]

            # 手动设置固定日期（例如 1212-12-12）做索引使用
            fixed_date = "2222-12-22"
            # fixed_date = latest_trade_date
            stock_info_df = temp_df[existing_columns].copy()  # 显式创建独立副本
            stock_info_df.loc[:, "date"] = fixed_date
            stock_info_df.loc[:, "date_int"] = stock_info_df["date"].astype(str).str.replace('-', '')
            if "market_id" in stock_info_df.columns and "code" in stock_info_df.columns:
                stock_info_df.loc[:, "code_market"] = (
                    stock_info_df["market_id"].astype(str) + "." +
                    stock_info_df["code"].astype(str).str.zfill(6)
                )
            # stock_info_df.loc[:, "industry"] = fixed_date
            print(f"[Success] stock_info_df：{stock_info_df}")
            print(f"[Success] stock_info_df-columns：{stock_info_df.columns}")

            
            # 2. 创建表（如果不存在）
            create_table_if_not_exists(stock_info)
            
            # 3. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, stock_info, stock_info_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 4. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=stock_info,
                    data=stock_info_df,
                    batch_size=1000  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                
                print(f"[Success] 股票基础信息表写入完成，数据量：{len(stock_info_df)}")
            except Exception as e:
                print(f"[Critical] 股票基础表批量写入失败: {str(e)}")

            return temp_df
        except Exception as e:
            print(f"[Error] 股票写入失败: {e}")
    except Exception as e:
        print(f"东方财富网-沪深京 A 股-实时行情处理失败: {e}")
        return pd.DataFrame()



#获取沪市A股+深市A股实时股票数据，接口请求数据
def fetch_zh_a_spot_data(
    url: str,
    params: Dict,
    page_size: int,
    page_param_name: str = "pn",  # 分页参数名应为页码参数（pn）
    start_page: int = 1           # 起始页码默认为1
    ) -> pd.DataFrame:

    first_page_params = {
        **params,
        "pz": page_size,          # 固定每页大小参数为pz
        page_param_name: start_page  # 起始页码
    }
    try:
        r = requests.get(url, params=first_page_params)
        r.raise_for_status()
        data_json = r.json()
        data_count = data_json["data"]["total"]
        page_size = len(data_json["data"]["diff"])
        page_total = math.ceil(data_count / page_size)
        data = data_json["data"]["diff"]
        print(f"[Debug] 总数据量: {data_count}, 总页数: {page_total}")
    except Exception as e:
        print(f"初始页获取失败: {str(e)}")
        return pd.DataFrame()

    # 2. 多线程请求剩余页
    def fetch_page(page: int) -> List[Dict]:
        page_params = {
            **params,
            "pz": page_size,       # 固定每页大小
            page_param_name: page   # 正确设置页码参数
        }
        try:
            r = requests.get(url, params=page_params)
            r.raise_for_status()
            return r.json()["data"]["diff"]
        except Exception as e:
            print(f"第{page}页请求失败: {e}")
            return []

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_page, page): page
            for page in range(start_page + 1, page_total + 1)  # 从第二页开始
        }

        # 进度条显示
        for future in tqdm(as_completed(futures), total=len(futures), desc="并发拉取数据"):
            page_data = future.result()
            if page_data:
                data.extend(page_data)

    return pd.DataFrame(data)


"""
定义公共函数：
create_table_if_not_exists(table_name)：检查数据表是否存在，如果不存在，则创建数据库并添加索引
同步表结构(conn, table_name, data_columns)： 动态检查并自动添加表中缺失的字段
sql语句生成器(table_name,data)：带入参数数据表名和数据，生成数据插入语句
execute_raw_sql(sql,params)：执行插入数据表
"""

#例句
def fetch_and_format_stock_info(sql):
    """执行SQL查询并格式化输出code_int.market_id"""
    sql = "SELECT market_id, code FROM cn_stock_info"
    conn = DBManager.get_new_connection()
    if not conn:
        print("数据库连接失败")
        return []
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        results = cursor.fetchall()
        
        # 格式化为 code.market_id
        formatted_results = [
            f"{row['code']}.{row['market_id']}"
            for row in results
            if 'code' in row and 'market_id' in row
        ]
        
        # 打印结果（或根据需求保存到文件）
        print("\n".join(formatted_results))
        return formatted_results
        
    except Error as e:
        print(f"查询失败: {e}")
        return []
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()



class DBManager:
    @staticmethod
    def get_new_connection():
        """创建并返回一个新的数据库连接"""
        try:
            connection = mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_database,
                charset=db_charset
            )
            return connection
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None

    @staticmethod
    def execute_sql(sql: str, params=None):
        """安全执行 SQL 语句"""
        connection = DBManager.get_new_connection()
        if connection:
            try:
                cursor = connection.cursor(buffered=True)
                cursor.execute(sql, params)
                connection.commit()
                cursor.close()
            except Error as e:
                print(f"Error while executing SQL: {e}")
            finally:
                if connection.is_connected():
                    connection.close()


def create_table_if_not_exists(table_name):
    # 创建表（不含索引）
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `date` DATE,
            `date_int` INT,
            `code_int` INT,
            `code` VARCHAR(6),
            `name` VARCHAR(20)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """
    DBManager.execute_sql(create_table_sql)

    # 检查并添加id列（如果不存在）
    conn = DBManager.get_new_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # 检查id列是否存在
            check_sql = f"""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{table_name}'
                  AND COLUMN_NAME = 'id';
            """
            cursor.execute(check_sql)
            count = cursor.fetchone()[0]
            if count == 0:
                # 添加id列
                alter_sql = f"""
                    ALTER TABLE `{table_name}`
                    ADD COLUMN `id` INT AUTO_INCREMENT PRIMARY KEY FIRST;
                """
                cursor.execute(alter_sql)
                conn.commit()
                print(f"表 {table_name} 成功添加 id 列")
        except Error as e:
            print(f"检查或添加 id 列失败: {e}")
            conn.rollback()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    # 创建索引（修复 Unread result found 问题）
    def create_index(index_name, columns, is_unique=False):
        conn = DBManager.get_new_connection()
        try:
            cursor = conn.cursor()
            # 检查索引是否存在
            check_sql = f"""
                SELECT COUNT(*)
                FROM information_schema.STATISTICS
                WHERE table_name = '{table_name}'
                  AND index_name = '{index_name}';
            """
            cursor.execute(check_sql)
            result = cursor.fetchall()  # 强制读取结果
            if result[0][0] == 0:
                index_type = "UNIQUE" if is_unique else ""
                create_sql = f"""
                    CREATE {index_type} INDEX `{index_name}`
                    ON `{table_name}` ({', '.join(columns)});
                """
                cursor.execute(create_sql)
                conn.commit()
        except Error as e:
            print(f"创建索引 {index_name} 失败: {e}")
            conn.rollback()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    # 添加索引
    create_index("idx_date_code_int", ["date_int", "code_int"], is_unique=True)
    create_index("idx_code_int", ["code_int"])
    create_index("idx_date_int", ["date_int"])


def 同步表结构(conn, table_name, data_columns):
    """动态添加缺失字段，附带调试日志和错误处理"""
    try:
        cursor = conn.cursor(buffered=True)
        
        # 调试：打印基本信息
        # print(f"\n[DEBUG] 开始同步表结构：{table_name}")
        # print(f"[DEBUG] 数据列要求字段：{data_columns}")

        # 获取现有字段
        cursor.execute(f"DESCRIBE `{table_name}`;")
        existing_columns = [row[0] for row in cursor.fetchall()]
        # print(f"[DEBUG] 数据库现有字段：{existing_columns}")

        # 获取配置表字段
        table_config = tbs.TABLE_REGISTRY.get(table_name, {})
        all_required_columns = list(table_config.get('columns', {}).keys())
        # print(f"[DEBUG] 配置表要求字段：{all_required_columns}")

        # 遍历处理字段
        for col in data_columns:
            try:
                # print(f"\n[DEBUG] 正在检查字段：{col}")
                
                if col not in existing_columns:
                    if col in all_required_columns:
                        # 从配置获取字段类型
                        col_info = table_config['columns'][col]
                        sql_type = tbs._get_sql_type(col_info['type'])
                        
                        # 执行添加字段
                        alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {sql_type};"
                        print(f"[EXECUTE] 执行SQL：{alter_sql}")
                        
                        cursor.execute(alter_sql)
                        # print(f"[SUCCESS] 字段 {col} 添加成功")
                    else:
                        print(f"[WARNING] 字段 {col} 不在配置表中，已跳过")
                else:
                    pass
                    # print(f"[INFO] 字段 {col} 已存在，无需添加")
            except Exception as col_error:
                print(f"[ERROR] 处理字段 {col} 时发生错误：{str(col_error)}")
                conn.rollback()  # 回滚当前字段操作

        conn.commit()
        print(f"[SUCCESS] 表 {table_name} 结构同步完成")

    except Exception as main_error:
        print(f"[CRITICAL] 同步表结构主流程失败：{str(main_error)}")
        conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            print("[INFO] 数据库游标已关闭")


def sql语句生成器(table_name, data, batch_size=500):
    # 预处理code_int字段
    if 'code' in data.columns and 'code_int' not in data.columns:
        data.insert(0, 'code_int', data['code'].astype(int))

    # SQL模板（批量版本）
    sql_template = """INSERT INTO `{table_name}` ({columns}) 
        VALUES {values}
        ON DUPLICATE KEY UPDATE {update_clause};"""

    # 定义字段和更新子句
    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'code_int']
    update_clause = ', '.join(
        [f"`{col}`=VALUES(`{col}`)" 
         for col in data.columns if col not in unique_keys]
    )

    # 分批次处理数据
    batches = []
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        value_rows = []

        for row in batch.itertuples(index=False):
            values = []
            for item in row:
                # 处理空值和特殊字符
                if pd.isna(item) or item in ['-', '']:
                    values.append("NULL")
                elif isinstance(item, (datetime.date, datetime.datetime)):
                    values.append(f"'{item.strftime('%Y-%m-%d')}'")
                elif isinstance(item, (int, float)):
                    values.append(str(item))
                else:
                    cleaned = str(item).replace("'", "''").replace("\\", "\\\\")
                    values.append(f"'{cleaned}'")
            value_rows.append(f"({', '.join(values)})")

        # 合并为单个VALUES子句
        values_str = ',\n'.join(value_rows)
        batches.append(
            sql_template.format(
                table_name=table_name,
                columns=columns,
                values=values_str,
                update_clause=update_clause
            )
        )
    
    return batches

def sql_batch_generator(table_name, data, batch_size=500):
    """通用批量SQL生成器"""
    if 'code' in data.columns and 'code_int' not in data.columns:
        data.insert(0, 'code_int', data['code'].astype(int))

    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'code_int']
    update_clause = ', '.join(
        [f"`{col}`=VALUES(`{col}`)" 
         for col in data.columns if col not in unique_keys]
    )

    batches = []
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        value_rows = []

        for row in batch.itertuples(index=False):
            values = []
            for item in row:
                if pd.isna(item) or item in ['-', '']:
                    values.append("NULL")
                elif isinstance(item, (datetime.date, datetime.datetime)):
                    values.append(f"'{item.strftime('%Y-%m-%d')}'")
                elif isinstance(item, (int, float)):
                    values.append(str(item))
                else:
                    cleaned = str(item).replace("'", "''").replace("\\", "\\\\")
                    values.append(f"'{cleaned}'")
            value_rows.append(f"({', '.join(values)})")

        sql = f"""INSERT INTO `{table_name}` ({columns}) 
               VALUES {','.join(value_rows)}
               ON DUPLICATE KEY UPDATE {update_clause};"""
        batches.append(sql)
    
    return batches

def execute_batch_sql(sql_batches, max_retries=3):
    """通用批量执行函数"""
    for batch in sql_batches:
        attempt = 0
        while attempt < max_retries:
            conn = None
            try:
                conn = DBManager.get_new_connection()
                cursor = conn.cursor()
                
                # 执行单个批次
                cursor.execute(batch)
                
                # 显式消费结果集
                while True:
                    if cursor.with_rows:
                        cursor.fetchall()
                    if not cursor.nextset():
                        break
                
                conn.commit()
                print(f"成功插入 {batch.count('VALUES')} 行数据")
                break
                
            except Error as e:
                attempt += 1
                print(f"第{attempt}次重试失败: {e}")
                if conn:
                    conn.rollback()
                time.sleep(2**attempt)  # 指数退避
            finally:
                if conn and conn.is_connected():
                    cursor.close()
                    conn.close()




def execute_raw_sql(sql, params=None, max_query_size=1024*1024, batch_size=500):
    """改进后的SQL执行函数，解决Commands out of sync问题"""
    connection = DBManager.get_new_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor(buffered=True)  # 使用缓冲游标
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        for statement in statements:
            try:
                # 执行当前语句
                cursor.execute(statement)
                # 显式消费所有结果集（关键修复）
                while True:
                    if cursor.with_rows:
                        cursor.fetchall()  # 消费结果集
                    if not cursor.nextset():
                        break
            except Error as e:
                # 更详细的错误日志
                print(f"执行失败: {statement[:100]}... | 错误类型: {type(e).__name__} | 错误详情: {str(e)}")
                connection.rollback()
                return False
            except Exception as ex:
                print(f"未知错误: {ex}")
                connection.rollback()
                return False
        connection.commit()  # 所有语句执行成功后提交
        return True
    except Error as e:
        print(f"数据库错误: {e}")
        connection.rollback()
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("数据库连接已安全关闭")  # 调试日志

def main():
    # 最新选股器，沪深全数据 OK
    stock_selection()

    # 实时股票 OK
    stock_zh_a_spot_em()

    # 实时ETF基金 OK
    etf_spot_em()

    # 实时指数 OK
    index_zh_a_spot_em()




# main函数入口
if __name__ == '__main__':
    main()

    # 最新选股器，沪深全数据 OK
    # stock_selection()

    # 实时股票 OK
    # stock_zh_a_spot_em()

    # 实时ETF基金 OK
    # etf_spot_em()

    # 实时指数 OK
    # index_zh_a_spot_em()

    # 历史日周月股票 OK
    # fetch_all_stock_hist()
    
    # th = fetch_stocks_trade_date().get_data()
    # print(f'{th}')