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
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset
import stock_zijin as stock_zijin
import indicators_data_daily as indicators_data_daily
import threeday_indicators as threeday_indicators
import market_sentiment_a as market_sentiment_a
import industry_data as industry_data
import industry_sentiment_a as industry_sentiment_a
import strategy_stock_buy as indicators_strategy_buy
import buy_20250425 as buy_20250425
import basic_data as basic_data
import realtime_indicators as indicators_now

numeric_cols = ["f2", "f3", "f4", "f5", "f6", "f7", "f8", "f10", "f15", "f16", "f17", "f18", "f22", "f11", "f24", "f25", "f9", "f115", "f114", "f23", "f112", "f113", "f61", "f48", "f37", "f49", "f57", "f40", "f41", "f45", "f46", "f38", "f39", "f20", "f21" ]
date_cols = ["f26", "f221"]

def is_open(price):
    return not np.isnan(price)

CN_INDUSTRY_SPOT = {
    'name': 'cn_industry_spot',
    'cn': '每日行业数据',
    'columns': {
        'id' : {'type': INT, 'cn': 'id', 'size': 0, 'en': 'id'},
        'date': {'map': None, 'type': DATE, 'cn': '日期', 'size': 90, 'en': 'date'},
        'date_int': {'map': None, 'type': INT, 'cn': '日期_int', 'size': 8, 'en': 'date_int'},
        'code': {'map': 'f12',  'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'en': 'code'},
        'name': {'map': 'f14', 'type': VARCHAR(20), 'cn': '名称', 'size': 120, 'en': 'name'},
        # 'code_int': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code_int'},
        'market_id':{'map': 'f13', 'type': INT, 'cn': '市场标识', 'size': 70,  'en': 'market_id'},
        'close': {'map': 'f2', 'type': FLOAT, 'cn': '最新价/收盘价', 'size': 70, 'en': 'close'},
        'change_rate': {'map': 'f3',  'type': FLOAT, 'cn': '涨跌幅', 'size': 70, 'en': 'change_rate'},
        'ups_downs': {'map': 'f4',  'type': FLOAT, 'cn': '涨跌额', 'size': 70, 'en': 'ups_downs'},
        'volume': {'map': 'f5',  'type': BIGINT, 'cn': '成交量', 'size': 90, 'en': 'volume'},
        'amount': {'map': 'f6',  'type': BIGINT, 'cn': '成交额', 'size': 100, 'en': 'amount'},
        'amplitude': {'type': FLOAT, 'cn': '振幅', 'size': 70, 'map': 'f7', 'en': 'amplitude'},
        'open': {'map': 'f17',  'type': FLOAT, 'cn': '开盘价', 'size': 70, 'en': 'open'},
        'high': {'map': 'f15',  'type': FLOAT, 'cn': '最高价', 'size': 70, 'en': 'high'},
        'low': {'map': 'f16',  'type': FLOAT, 'cn': '最低价', 'size': 70, 'en': 'low'},
        'pre_close_price': {'map': 'f18', 'type': FLOAT, 'cn': '昨收', 'size': 70, 'en': 'pre_close_price'},
        'turnover': {'map': 'f8', 'type': FLOAT, 'cn': '换手率', 'size': 70, 'en': 'turnover'},
        'dtsyl': { 'map': 'f9', 'type': FLOAT, 'cn': '市盈率动', 'size': 70,'en': 'dtsyl'},
        'volume_ratio': { 'map': 'f10','type': FLOAT, 'cn': '量比', 'size': 70, 'en': 'volume_ratio'},
        'speed_increase_5': { 'map': 'f11', 'type': FLOAT, 'cn': '5分钟涨跌', 'size': 70,'en': 'speed_increase_5'},
        'total_market_cap': {'map': 'f20', 'type': BIGINT, 'cn': '总市值', 'size': 120, 'en': 'total_market_cap'},
        'free_cap': {'map': 'f21', 'type': BIGINT, 'cn': '流通市值', 'size': 120, 'en': 'free_cap'},
        'speed_increase': {'map': 'f22','type': FLOAT, 'cn': '涨速', 'size': 70,  'en': 'speed_increase'},
        'speed_increase_60': {'map': 'f24','type': FLOAT, 'cn': '60日涨跌幅', 'size': 70,  'en': 'speed_increase_60'},
        'speed_increase_all': {'map': 'f25','type': FLOAT, 'cn': '年初至今涨跌幅', 'size': 70,  'en': 'speed_increase_all'},
        'listing_date': { 'map': 'f26','type': DATE, 'cn': '上市时间', 'size': 110, 'en': 'listing_date'},
        'total_shares': { 'map': 'f38', 'type': BIGINT, 'cn': '总股本', 'size': 120,'en': 'total_shares'},
        'free_shares': {'map': 'f39','type': BIGINT, 'cn': '已流通股份', 'size': 120,  'en': 'free_shares'},
        'up_stocks': {'type': INT, 'cn': '上涨家数', 'size': 70, 'map': 'f104', 'en': 'up_stocks'},
        'down_stocks': {'type': INT, 'cn': '下跌家数', 'size': 70, 'map': 'f105', 'en': 'down_stocks'},
        'line_stocks': {'type': INT, 'cn': '持平家数', 'size': 70, 'map': 'f106', 'en': 'line_stocks'},
        'leading_stocks': {'type': VARCHAR(20), 'cn': '领涨股', 'size': 120, 'map': 'f128', 'en': 'leading_stocks'},
        'leading_stocks_code': {'type': VARCHAR(6), 'cn': '领涨股代码', 'size': 120, 'map': 'f140', 'en': 'leading_stocks_code'},
        'leading_stocks_marketid': {'type': INT, 'cn': '领涨股市场标识', 'size': 120, 'map': 'f141', 'en': 'leading_stocks_marketid'},
        'leading_stocks_change_rate': {'type': FLOAT, 'cn': '领涨股涨幅', 'size': 70, 'map': 'f136', 'en': 'leading_stocks_change_rate'},
        'declining_stocks': {'type': VARCHAR(20), 'cn': '领跌股', 'size': 120, 'map': 'f207', 'en': 'declining_stocks'},
        'declining_stocks_code': {'type': VARCHAR(6), 'cn': '领跌股代码', 'size': 120, 'map': 'f208', 'en': 'declining_stocks_code'},
        'declining_stocks_marketid': {'type': INT, 'cn': '领跌股市场标识', 'size': 120, 'map': 'f209', 'en': 'declining_stocks_marketid'},
        'declining_stocks_change_rate': {'type': FLOAT, 'cn': '领跌股跌幅', 'size': 70, 'map': 'f222', 'en': 'declining_stocks_change_rate'},
    }
}

def industry_zh_a_spot_em() -> pd.DataFrame:
    '''
    东方财富网-沪深京 A 股-行业实时行情
    https://quote.eastmoney.com/center/gridlist.html#industry_board
    :return: 行业实时行情
    :rtype: pandas.DataFrame
    '''
    #实时行情数据主表
    table_name = CN_INDUSTRY_SPOT['name']
    table_name_cols = CN_INDUSTRY_SPOT['columns']

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
        "fs": "m:90+t:2+f:!50",
        # "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "fields": fields,
        "_": "1623833739532",
    }
    try:
        temp_df = fetch_zh_a_spot_data(url,params,page_size,"pn")# 将数据中NaN空数据进行替换：替换np.nan为None

        # 定义数值列清单
        numeric_cols = [
            'f2','f3','f4','f5','f6','f7','f8','f9','f10','f11',
            'f15','f16','f17','f18','f20','f21','f22','f24','f25',
            'f38','f39','f104','f105','f106','f141', 'f136', 'f209','f222',
        ]

        # 执行类型转换
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        date_cols = ["f26"]
        temp_df[date_cols] = temp_df[date_cols].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce"))
        temp_df.rename(columns=en_name, inplace=True) # 将默认列名改为英文列名

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        temp_df.loc[:, "date"] = latest_trade_date
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

        temp_df = temp_df.loc[temp_df['close'].apply(is_open)]
        temp_df = temp_df.replace({np.nan: None})

        industry_data_df = pd.DataFrame()  # 显式创建独立副本
        industry_data_df.loc[:, "date"] = temp_df["date"]
        industry_data_df.loc[:, "date_int"] = temp_df["date_int"]
        industry_data_df.loc[:, "code"] = temp_df["code"]
        # industry_data_df.loc[:, "code_int"] = temp_df["code"].astype(int)
        industry_data_df.loc[:, "name"] = temp_df["name"]
        industry_data_df.loc[:, "open"] = temp_df["open"]
        industry_data_df.loc[:, "close"] = temp_df["close"]
        industry_data_df.loc[:, "high"] = temp_df["high"]
        industry_data_df.loc[:, "low"] = temp_df["low"]
        industry_data_df.loc[:, "volume"] = temp_df["volume"]
        industry_data_df.loc[:, "turnover"] = temp_df["turnover"]

        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        print(f'行业实时行情数据主表{industry_data_df}')
        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        # print(f'实时行情数据主表{temp_df}')

        # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            try:
                # 生成批量SQL
                sql_batches = industry_sql_batch_generator(
                    table_name='cn_industry_hist_daily',
                    data=industry_data_df,
                    batch_size=1000  # 根据实际情况调整
                )

                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 行业行情主表批量写入完成，数据量：{len(industry_data_df)}")
            except Exception as e:
                print(f"[Critical] 行业行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 行业主表写入失败: {e}")
        #################################################

    except Exception as e:
        print(f"东方财富网-沪深京 A 股-实时行业行情处理失败: {e}")
        return pd.DataFrame()


def industry_sql_batch_generator(table_name, data, batch_size=500):
    """通用批量SQL生成器"""

    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'name']
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


########################################
#获取沪市A股+深市A股实时股票数据数据并写入数据库

CN_STOCK_SPOT = {
    'name': 'cn_stock_spot',
    'cn': '每日股票数据',
    'columns': {
        #'id' : {'map': None,'type': INT, 'cn': 'id', 'size': 0, 'en': 'id'},
        'date': {'type': DATE, 'cn': '日期', 'size': 90, 'map': None, 'en': 'date'},
        'date_int': {'type': INT, 'cn': '日期_int', 'size': 8, 'map': None,'en': 'date_int'},
        'code': {'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'map': 'f12', 'en': 'code'},
        'code_int': {'type': INT, 'cn': '代码_int', 'size': 10, 'map': None, 'en': 'code_int'},
        'name': {'type': VARCHAR(20), 'cn': '代码', 'size': 70, 'map': 'f14', 'en': 'name'},
        'market_id':{'type': INT, 'cn': '市场标识', 'size': 10, 'map': 'f13', 'en': 'market_id'},
        'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70, 'map': 'f2', 'en': 'new_price'},
        'change_rate': {'type': FLOAT, 'cn': '涨跌幅', 'size': 70, 'map': 'f3', 'en': 'change_rate'},
        'ups_downs': {'type': FLOAT, 'cn': '涨跌额', 'size': 70, 'map': 'f4', 'en': 'ups_downs'},
        'volume': {'type': BIGINT, 'cn': '成交量', 'size': 90, 'map': 'f5', 'en': 'volume'},
        'deal_amount': {'type': BIGINT, 'cn': '成交额', 'size': 100, 'map': 'f6', 'en': 'deal_amount'},
        'amplitude': {'type': FLOAT, 'cn': '振幅', 'size': 70, 'map': 'f7', 'en': 'amplitude'},
        'turnoverrate': {'type': FLOAT, 'cn': '换手率', 'size': 70, 'map': 'f8', 'en': 'turnoverrate'},
        'volume_ratio': {'type': FLOAT, 'cn': '量比', 'size': 70, 'map': 'f10', 'en': 'volume_ratio'},
        'open_price': {'type': FLOAT, 'cn': '今开', 'size': 70, 'map': 'f17', 'en': 'open_price'},
        'high_price': {'type': FLOAT, 'cn': '最高', 'size': 70, 'map': 'f15', 'en': 'high_price'},
        'low_price': {'type': FLOAT, 'cn': '最低', 'size': 70, 'map': 'f16', 'en': 'low_price'},
        'pre_close_price': {'type': FLOAT, 'cn': '昨收', 'size': 70, 'map': 'f18', 'en': 'pre_close_price'},
        'speed_increase': {'type': FLOAT, 'cn': '涨速', 'size': 70, 'map': 'f22', 'en': 'speed_increase'},
        'speed_increase_5': {'type': FLOAT, 'cn': '5分钟涨跌', 'size': 70, 'map': 'f11', 'en': 'speed_increase_5'},
        'speed_increase_60': {'type': FLOAT, 'cn': '60日涨跌幅', 'size': 70, 'map': 'f24', 'en': 'speed_increase_60'},
        'speed_increase_all': {'type': FLOAT, 'cn': '年初至今涨跌幅', 'size': 70, 'map': 'f25', 'en': 'speed_increase_all'},
        'dtsyl': {'type': FLOAT, 'cn': '市盈率动', 'size': 70, 'map': 'f9', 'en': 'dtsyl'},
        'pe9': {'type': FLOAT, 'cn': '市盈率TTM', 'size': 70, 'map': 'f115', 'en': 'pe9'},
        'pe': {'type': FLOAT, 'cn': '市盈率静', 'size': 70, 'map': 'f114', 'en': 'pe'},
        'pbnewmrq': {'type': FLOAT, 'cn': '市净率', 'size': 70, 'map': 'f23', 'en': 'pbnewmrq'},
        'basic_eps': {'type': FLOAT, 'cn': '每股收益', 'size': 70, 'map': 'f112', 'en': 'basic_eps'},
        'bvps': {'type': FLOAT, 'cn': '每股净资产', 'size': 70, 'map': 'f113', 'en': 'bvps'},
        'per_capital_reserve': {'type': FLOAT, 'cn': '每股公积金', 'size': 70, 'map': 'f61', 'en': 'per_capital_reserve'},
        'per_unassign_profit': {'type': FLOAT, 'cn': '每股未分配利润', 'size': 70, 'map': 'f48', 'en': 'per_unassign_profit'},
        'roe_weight': {'type': FLOAT, 'cn': '加权净资产收益率', 'size': 70, 'map': 'f37', 'en': 'roe_weight'},
        'sale_gpr': {'type': FLOAT, 'cn': '毛利率', 'size': 70, 'map': 'f49', 'en': 'sale_gpr'},
        'debt_asset_ratio': {'type': FLOAT, 'cn': '资产负债率', 'size': 70, 'map': 'f57', 'en': 'debt_asset_ratio'},
        'total_operate_income': {'type': BIGINT, 'cn': '营业收入', 'size': 120, 'map': 'f40', 'en': 'total_operate_income'},
        'toi_yoy_ratio': {'type': FLOAT, 'cn': '营业收入同比增长', 'size': 70, 'map': 'f41', 'en': 'toi_yoy_ratio'},
        'parent_netprofit': {'type': BIGINT, 'cn': '归属净利润', 'size': 110, 'map': 'f45', 'en': 'parent_netprofit'},
        'netprofit_yoy_ratio': {'type': FLOAT, 'cn': '归属净利润同比增长', 'size': 70, 'map': 'f46', 'en': 'netprofit_yoy_ratio'},
        'report_date': {'type': DATE, 'cn': '报告期', 'size': 110, 'map': 'f221', 'en': 'report_date'},
        'total_shares': {'type': BIGINT, 'cn': '总股本', 'size': 120, 'map': 'f38', 'en': 'total_shares'},
        'free_shares': {'type': BIGINT, 'cn': '已流通股份', 'size': 120, 'map': 'f39', 'en': 'free_shares'},
        'total_market_cap': {'type': BIGINT, 'cn': '总市值', 'size': 120, 'map': 'f20', 'en': 'total_market_cap'},
        'free_cap': {'type': BIGINT, 'cn': '流通市值', 'size': 120, 'map': 'f21', 'en': 'free_cap'},
        'industry': {'type': VARCHAR(20), 'cn': '所处行业', 'size': 100, 'map': 'f100', 'en': 'industry'},
        'listing_date': {'type': DATE, 'cn': '上市时间', 'size': 110, 'map': 'f26', 'en': 'listing_date'}
    }
}

def stock_zh_a_spot_em() -> pd.DataFrame:
    '''
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    '''
    #实时行情数据主表
    table_name = CN_STOCK_SPOT['name']
    table_name_cols = CN_STOCK_SPOT['columns']

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

        stock_data_df = pd.DataFrame()  # 显式创建独立副本
        stock_data_df.loc[:, "date"] = temp_df["date"]
        stock_data_df.loc[:, "date_int"] = temp_df["date_int"]
        stock_data_df.loc[:, "code"] = temp_df["code"]
        stock_data_df.loc[:, "code_int"] = temp_df["code"].astype(int)
        stock_data_df.loc[:, "name"] = temp_df["name"]
        stock_data_df.loc[:, "open"] = temp_df["open_price"]
        stock_data_df.loc[:, "close"] = temp_df["new_price"]
        stock_data_df.loc[:, "high"] = temp_df["high_price"]
        stock_data_df.loc[:, "low"] = temp_df["low_price"]
        stock_data_df.loc[:, "volume"] = temp_df["volume"]
        stock_data_df.loc[:, "turnover"] = temp_df["turnoverrate"]
        # stock_data_df.loc[:, "amount"] = temp_df[""]

        print(f'实时行情数据主表{stock_data_df}')
        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        # print(f'实时行情数据主表{temp_df}')

        # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name='cn_stock_hist_daily',
                    data=stock_data_df,
                    batch_size=1000  # 根据实际情况调整
                )

                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 股票行情主表批量写入完成，数据量：{len(stock_data_df)}")
            except Exception as e:
                print(f"[Critical] 股票行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 股票主表写入失败: {e}")
        #################################################

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
#获取国内各大指数实时数据并写入数据库

CN_INDEX_SPOT = {
    'name': 'cn_index_spot',
    'cn': '每日指数数据',
    'columns': {
       # 'id' : {'map': None,'type': INT, 'cn': 'id', 'size': 0, 'en': 'id'},
        'date': {'map': None, 'type': DATE, 'cn': '日期', 'size': 90, 'en': 'date'},
        'date_int': {'map': None,'type': INT, 'cn': '日期_int', 'size': 8, 'en': 'date_int'},
        'code': {'map': 'f12',  'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'en': 'code'},
        'code_int': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code_int'},
        'name': {'map': 'f14', 'type': VARCHAR(20), 'cn': '名称', 'size': 120, 'en': 'name'},
        'market_id':{'map': 'f13', 'type': INT, 'cn': '市场标识', 'size': 70,  'en': 'market_id'},
        'new_price': {'map': 'f2', 'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
        'change_rate': {'map': 'f3',  'type': FLOAT, 'cn': '涨跌幅', 'size': 70, 'en': 'change_rate'},
        'ups_downs': {'map': 'f4',  'type': FLOAT, 'cn': '涨跌额', 'size': 70, 'en': 'ups_downs'},
        'volume': {'map': 'f5',  'type': BIGINT, 'cn': '成交量', 'size': 90, 'en': 'volume'},
        'deal_amount': {'map': 'f6',  'type': BIGINT, 'cn': '成交额', 'size': 100, 'en': 'deal_amount'},
        'amplitude': {'type': FLOAT, 'cn': '振幅', 'size': 70, 'map': 'f7', 'en': 'amplitude'},
        'open_price': {'map': 'f17',  'type': FLOAT, 'cn': '开盘价', 'size': 70, 'en': 'open_price'},
        'high_price': {'map': 'f15',  'type': FLOAT, 'cn': '最高价', 'size': 70, 'en': 'high_price'},
        'low_price': {'map': 'f16',  'type': FLOAT, 'cn': '最低价', 'size': 70, 'en': 'low_price'},
        'pre_close_price': {'map': 'f18', 'type': FLOAT, 'cn': '昨收', 'size': 70, 'en': 'pre_close_price'},
        'turnoverrate': {'map': 'f8', 'type': FLOAT, 'cn': '换手率', 'size': 70, 'en': 'turnoverrate'},
        'total_market_cap': {'map': 'f20', 'type': BIGINT, 'cn': '总市值', 'size': 120, 'en': 'total_market_cap'},
        'free_cap': {'map': 'f21', 'type': BIGINT, 'cn': '流通市值', 'size': 120, 'en': 'free_cap'}
    }
}

def index_zh_a_spot_em() -> pd.DataFrame:

    #实时行情数据主表
    table_name = CN_INDEX_SPOT['name'] # cn_index_spot
    table_name_cols = CN_INDEX_SPOT['columns']

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

        index_data_df = pd.DataFrame()  # 显式创建独立副本
        index_data_df.loc[:, "date"] = temp_df["date"]
        index_data_df.loc[:, "date_int"] = temp_df["date_int"]
        index_data_df.loc[:, "code"] = temp_df["code"]
        index_data_df.loc[:, "code_int"] = temp_df["code"].astype(int)
        index_data_df.loc[:, "name"] = temp_df["name"]
        index_data_df.loc[:, "open"] = temp_df["open_price"]
        index_data_df.loc[:, "close"] = temp_df["new_price"]
        index_data_df.loc[:, "high"] = temp_df["high_price"]
        index_data_df.loc[:, "low"] = temp_df["low_price"]
        index_data_df.loc[:, "volume"] = temp_df["volume"]
        index_data_df.loc[:, "turnover"] = temp_df["turnoverrate"]
        
        # stock_data_df.loc[:, "amount"] = temp_df[""]

        print(f'指数实时行情数据主表{index_data_df}')
        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        # print(f'实时行情数据主表{temp_df}')

        # ==== 主表（cn_stock_spot）写入逻辑 ====
        try:
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name='cn_index_hist_daily',
                    data=index_data_df,
                    batch_size=1000  # 根据实际情况调整
                )

                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 指数实时行情主表批量写入完成，数据量：{len(index_data_df)}")
            except Exception as e:
                print(f"[Critical] 指数实时行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 指数实时行情写入失败: {e}")
        #################################################

    except Exception as e:
        print(f"指数实时行情行情处理失败: {e}")
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
#获取ETF基金实时数据并写入数据库


CN_ETF_SPOT = {
    'name': 'cn_etf_spot',
    'cn': '每日ETF数据',
    'columns': {
        #'id' : {'type': INT, 'cn': 'id', 'size': 0, 'en': 'id'},
        'date': {'map': None, 'type': DATE, 'cn': '日期', 'size': 90, 'en': 'date'},
        'date_int': {'map': None, 'type': INT, 'cn': '日期_int', 'size': 8, 'en': 'date_int'},
        'code': {'map': 'f12',  'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'en': 'code'},
        'name': {'map': 'f14', 'type': VARCHAR(20), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code_int': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code_int'},
        'market_id':{'map': 'f13', 'type': INT, 'cn': '市场标识', 'size': 70,  'en': 'market_id'},
        'new_price': {'map': 'f2', 'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
        'change_rate': {'map': 'f3',  'type': FLOAT, 'cn': '涨跌幅', 'size': 70, 'en': 'change_rate'},
        'ups_downs': {'map': 'f4',  'type': FLOAT, 'cn': '涨跌额', 'size': 70, 'en': 'ups_downs'},
        'volume': {'map': 'f5',  'type': BIGINT, 'cn': '成交量', 'size': 90, 'en': 'volume'},
        'deal_amount': {'map': 'f6',  'type': BIGINT, 'cn': '成交额', 'size': 100, 'en': 'deal_amount'},
        'amplitude': {'type': FLOAT, 'cn': '振幅', 'size': 70, 'map': 'f7', 'en': 'amplitude'},
        'open_price': {'map': 'f17',  'type': FLOAT, 'cn': '开盘价', 'size': 70, 'en': 'open_price'},
        'high_price': {'map': 'f15',  'type': FLOAT, 'cn': '最高价', 'size': 70, 'en': 'high_price'},
        'low_price': {'map': 'f16',  'type': FLOAT, 'cn': '最低价', 'size': 70, 'en': 'low_price'},
        'pre_close_price': {'map': 'f18', 'type': FLOAT, 'cn': '昨收', 'size': 70, 'en': 'pre_close_price'},
        'turnoverrate': {'map': 'f8', 'type': FLOAT, 'cn': '换手率', 'size': 70, 'en': 'turnoverrate'},
        'total_market_cap': {'map': 'f20', 'type': BIGINT, 'cn': '总市值', 'size': 120, 'en': 'total_market_cap'},
        'free_cap': {'map': 'f21', 'type': BIGINT, 'cn': '流通市值', 'size': 120, 'en': 'free_cap'}
    }
}

def etf_spot_em() -> pd.DataFrame:
    """
    东方财富-ETF 实时行情
    https://quote.eastmoney.com/center/gridlist.html#fund_etf
    :return: ETF 实时行情
    :rtype: pandas.DataFrame
    """
    #实时行情数据主表
    table_name = CN_ETF_SPOT['name'] # cn_etf_spot
    table_name_cols = CN_ETF_SPOT['columns']

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

        etf_data_df = pd.DataFrame()  # 显式创建独立副本
        etf_data_df.loc[:, "date"] = temp_df["date"]
        etf_data_df.loc[:, "date_int"] = temp_df["date_int"]
        etf_data_df.loc[:, "code"] = temp_df["code"]
        etf_data_df.loc[:, "code_int"] = temp_df["code"].astype(int)
        etf_data_df.loc[:, "name"] = temp_df["name"]
        etf_data_df.loc[:, "open"] = temp_df["open_price"]
        etf_data_df.loc[:, "close"] = temp_df["new_price"]
        etf_data_df.loc[:, "high"] = temp_df["high_price"]
        etf_data_df.loc[:, "low"] = temp_df["low_price"]
        etf_data_df.loc[:, "volume"] = temp_df["volume"]
        etf_data_df.loc[:, "turnover"] = temp_df["turnoverrate"]
         # ==== 主表（cn_etf_spot）写入逻辑 ====
        try:
            
            # 3. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name="cn_etf_hist_daily",
                    data=etf_data_df,
                    batch_size=1200  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                
                print(f"[Success] ETF实时行主表批量写入完成，数据量：{len(etf_data_df)}")
            except Exception as e:
                print(f"[Critical] ETF实时行情主表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Critical] ETF批量写入失败: {str(e)}")
        #################################################


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

"""
定义公共函数：
create_table_if_not_exists(table_name)：检查数据表是否存在，如果不存在，则创建数据库并添加索引
同步表结构(conn, table_name, data_columns)： 动态检查并自动添加表中缺失的字段
sql语句生成器(table_name,data)：带入参数数据表名和数据，生成数据插入语句
execute_raw_sql(sql,params)：执行插入数据表
"""



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



def sql_batch_generator(table_name, data, batch_size=500):
    """通用批量SQL生成器"""
    # if 'code' in data.columns and 'code_int' not in data.columns:
    #     data.insert(0, 'code_int', data['code'].astype(int))

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
    # stock_zijin.main()
    # 实时股票 OK
    # stock_zh_a_spot_em()

    # index_zh_a_spot_em()

    # etf_spot_em()

    # industry_zh_a_spot_em()

    #基础数据脚本会将实时数据回填到实时表、基础信息表、历史数据表
    basic_data.main()

    indicators_now.main()

    threeday_indicators.main()

    market_sentiment_a.main()

    industry_data.main()

    industry_sentiment_a.main()

    indicators_strategy_buy.main()

    buy_20250425.main()




# main函数入口
if __name__ == '__main__':
    main()
