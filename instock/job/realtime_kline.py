#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import json
import re
import math
import requests
import numpy as np
import pandas as pd
import time
import datetime 
import mysql.connector
import pandas_market_calendars as mcal
from mysql.connector import Error
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 


numeric_cols = ["f2", "f3", "f4", "f5", "f6", "f7", "f8", "f10", "f15", "f16", "f17", "f18", "f22", "f11", "f24", "f25", "f9", "f115", "f114", "f23", "f112", "f113", "f61", "f48", "f37", "f49", "f57", "f40", "f41", "f45", "f46", "f38", "f39", "f20", "f21" ]
date_cols = ["f26", "f221"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive"
}

def is_open(price):
    return not np.isnan(price)


# 获取新浪接口实时股票数据保存到实时数据库表和历史k线表
# 网址：https://vip.stock.finance.sina.com.cn/mkt/#hs_a
# 接口：https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=55&num=100&sort=symbol&asc=0&node=hs_a&symbol=
# hs_a，沪深A股，最多55页（包含北京）

def fetch_sina_stock_data(page):
    """从新浪接口获取股票数据"""
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=0&node=hs_a"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://vip.stock.finance.sina.com.cn/mkt/'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # 新浪返回的是JSONP格式，实际是JSON字符串
        data_str = response.text.strip()

        # 处理特殊JSON格式（无引号的key）
        try:
            # 尝试直接解析
            return json.loads(data_str)
        except json.JSONDecodeError:
            # 修复非法JSON：给key加双引号
            fixed_json = re.sub(r'(\w+):', r'"\1":', data_str)
            return json.loads(fixed_json)

    except Exception as e:
        print(f"第{page}页请求失败: {str(e)}")
        return None


def stock_hs_a_spot_sina():
    """获取沪深A股实时数据并返回DataFrame"""
    total_pages = 55  # 沪深A股总页数
    all_stocks = []  # 存储所有股票数据

    for page in range(1, total_pages + 1):
        print(f"正在获取第 {page}/{total_pages} 页数据...")

        stock_data = fetch_sina_stock_data(page)
        if not stock_data:
            print(f"第{page}页未获取到有效数据，跳过")
            continue

        print(f"获取到第{page}页数据，共{len(stock_data)}条记录")
        all_stocks.extend(stock_data)

        # 添加延迟避免被封IP
        time.sleep(1.5)

    print("所有数据获取完成！")
    print(f"总共获取到 {len(all_stocks)} 条股票数据")

    # 转换为DataFrame
    temp_df = pd.DataFrame(all_stocks)

    # 数据清洗和类型转换
    if not temp_df.empty:
        # 重命名列名为中文（可选）
        column_mapping = {
            'symbol': '股票代码',
            'name': '股票名称',
            'trade': '最新价',
            'pricechange': '涨跌额',
            'changepercent': '涨跌幅',
            'buy': '买入价',
            'sell': '卖出价',
            'settlement': '昨收',
            'open': '今开',
            'high': '最高',
            'low': '最低',
            'volume': '成交量(股)',  # 原始单位是股
            'amount': '成交额', # 元
            'ticktime': '时间',
            'code': '内部代码',
            'turnoverratio':'换手率',
            'nmc':'流通值',
            'mktcap':'总市值',
            'pb':'市净率',
            'per': '市盈率'
        }
        temp_df = temp_df.rename(columns=column_mapping)

        # 转换数值类型（去除逗号）
        numeric_cols = ['最新价', '涨跌额', '涨跌幅', '买入价', '卖出价',
                       '昨收', '今开', '最高', '最低', '成交量(股)', '成交额',
                       '换手率', '流通值', '总市值', '市净率', '市盈率']

        for col in numeric_cols:
            if col in temp_df.columns:
                # 处理可能的字符串类型（包含逗号）
                temp_df[col] = temp_df[col].apply(lambda x: float(str(x).replace(',', '')) if pd.notnull(x) else None)

        temp_df['成交量(手)'] = temp_df['成交量(股)'] / 100

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        temp_df.loc[:, "date"] = latest_trade_date
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')


        # 准备历史K线数据
        stock_kline_df = pd.DataFrame({
            "date": temp_df["date"],
            "date_int": temp_df["date_int"],
            "code": temp_df["内部代码"],
            "code_int": temp_df["内部代码"].astype(int),
            "name": temp_df["股票名称"],
            "open": temp_df["今开"],
            "close": temp_df["最新价"],
            "high": temp_df["最高"],
            "low": temp_df["最低"],
            # 使用转换后的成交量(手)
            "volume": temp_df["成交量(手)"]
        })


        print(stock_kline_df.head())

        # 1. 同步表结构
        table_name = 'kline_stock'
        data_columns = list(stock_kline_df.columns)
        sync_table_structure(table_name, data_columns)

        # 生成批量SQL
        sql_batches = sql_batch_generator(
            table_name='kline_stock',
            data=stock_kline_df,
            batch_size=6000  # 根据实际情况调整
        )

        # 执行批量插入
        execute_batch_sql(sql_batches)
        print(f"[Success] 股票实时K线数据写入完成，数据量：{len(stock_kline_df)}")

    return temp_df


# 获取新浪接口实时大盘指数数据保存到数据库表和历史k线表
# 网址：https://vip.stock.finance.sina.com.cn/mkt/#hs_a
# 接口：https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=100&sort=symbol&asc=0&node=dpzs&symbol=
# hs_a，沪深A股，最多55页（包含北京）

def fetch_sina_index_data(page):
    """从新浪接口获取股票数据"""
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=0&node=dpzs"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://vip.stock.finance.sina.com.cn/mkt/'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # 新浪返回的是JSONP格式，实际是JSON字符串
        data_str = response.text.strip()

        # 处理特殊JSON格式（无引号的key）
        try:
            # 尝试直接解析
            return json.loads(data_str)
        except json.JSONDecodeError:
            # 修复非法JSON：给key加双引号
            fixed_json = re.sub(r'(\w+):', r'"\1":', data_str)
            return json.loads(fixed_json)

    except Exception as e:
        print(f"第{page}页请求失败: {str(e)}")
        return None


def index_dpzs_spot_sina():
    """获取沪深A股实时数据并返回DataFrame"""
    total_pages = 1  # 沪深A股总页数
    all_stocks = []  # 存储所有股票数据

    for page in range(1, total_pages + 1):
        print(f"正在获取第 {page}/{total_pages} 页数据...")

        stock_data = fetch_sina_index_data(page)
        if not stock_data:
            print(f"第{page}页未获取到有效数据，跳过")
            continue

        print(f"获取到第{page}页数据，共{len(stock_data)}条记录")
        all_stocks.extend(stock_data)

        # 添加延迟避免被封IP
        time.sleep(1.5)

    print("所有数据获取完成！")
    print(f"总共获取到 {len(all_stocks)} 条指数数据")

    # 转换为DataFrame
    temp_df = pd.DataFrame(all_stocks)

    # 数据清洗和类型转换
    if not temp_df.empty:
        # 重命名列名为中文（可选）
        column_mapping = {
            'symbol': '股票代码',
            'name': '股票名称',
            'trade': '最新价',
            'pricechange': '涨跌额',
            'changepercent': '涨跌幅',
            'buy': '买入价',
            'sell': '卖出价',
            'settlement': '昨收',
            'open': '今开',
            'high': '最高',
            'low': '最低',
            'volume': '成交量(股)',  # 原始单位是股
            'amount': '成交额',
            'ticktime': '时间',
            'code': '内部代码',
            'turnoverratio':'换手率',
            'nmc':'流通值',
            'mktcap':'总市值',
            'pb':'市净率',
            'per': '市盈率'
        }
        temp_df = temp_df.rename(columns=column_mapping)

        # 转换数值类型（去除逗号）
        numeric_cols = ['最新价', '涨跌额', '涨跌幅', '买入价', '卖出价',
                       '昨收', '今开', '最高', '最低', '成交量(股)', '成交额',
                       '换手率', '流通值', '总市值', '市净率', '市盈率']

        for col in numeric_cols:
            if col in temp_df.columns:
                # 处理可能的字符串类型（包含逗号）
                temp_df[col] = temp_df[col].apply(lambda x: float(str(x).replace(',', '')) if pd.notnull(x) else None)

        temp_df['成交量(手)'] = temp_df['成交量(股)'] / 100

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime("%Y-%m-%d")
        temp_df.loc[:, "date"] = latest_trade_date
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')


        # 准备历史K线数据
        index_kline_df = pd.DataFrame({
            "date": temp_df["date"],
            "date_int": temp_df["date_int"],
            "code": temp_df["内部代码"],
            "code_int": temp_df["内部代码"].astype(int),
            "name": temp_df["股票名称"],
            "open": temp_df["今开"],
            "close": temp_df["最新价"],
            "high": temp_df["最高"],
            "low": temp_df["最低"],
            # 使用转换后的成交量(手)
            "volume": temp_df["成交量(手)"]
        })


        print(index_kline_df.head())

        # 1. 同步表结构
        table_name = 'kline_index'
        data_columns = list(index_kline_df.columns)
        sync_table_structure(table_name, data_columns)

        # 生成批量SQL
        sql_batches = sql_batch_generator(
            table_name='kline_index',
            data=index_kline_df,
            batch_size=6000  # 根据实际情况调整
        )

        # 执行批量插入
        execute_batch_sql(sql_batches)
        print(f"[Success] 大盘指数实时K线数据写入完成，数据量：{len(index_kline_df)}")

    return temp_df






########################################
#腾讯接口获取股票实时数据并写入数据库

def fetch_tencent_stock_data(offset=0, count=200):
    """获取单页股票数据"""
    url = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
    params = {
        "_appver": "11.17.0",
        "board_code": "aStock",
        "sort_type": "price",
        "direct": "down",
        "offset": offset,
        "count": count
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['code'] == 0:
                return data['data']['rank_list'], data['data']['total']
        return [], 0
    except Exception as e:
        print(f"请求失败: {e}")
        return [], 0


def get_tencent_all_stocks():
    """获取所有股票数据"""
    all_stocks = []
    count = 200  # 每页固定200条
    first_page_data, total = fetch_tencent_stock_data(offset=0, count=count)
    all_stocks.extend(first_page_data)

    if total > count:
        total_pages = (total + count - 1) // count  # 计算总页数
        for page in tqdm(range(1, total_pages), desc="获取股票数据进度"):
            offset = page * count
            page_data, _ = fetch_tencent_stock_data(offset=offset, count=count)
            all_stocks.extend(page_data)

    # 创建DataFrame
    df = pd.DataFrame(all_stocks)

    # 获取上证交易所日历
    sh_cal = mcal.get_calendar('SSE')
    latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime(
        "%Y-%m-%d")
    df.loc[:, "date"] = latest_trade_date

    # 中文字段名映射
    column_mapping = {
        'date': '日期',
        'code': '股票代码',
        'hsl': '换手率',
        'lb': '量比',
        'ltsz': '流通市值', # (亿元)
        'name': '股票名称',
        'pe_ttm': '动态市盈率',
        'pn': '市净率',
        'speed': '涨速', # (每分钟)
        'state': '状态',
        'stock_type': '股票类型',
        'turnover': '成交额', # (万元)
        'volume': '成交量', # (手)
        'zd': '涨跌额',
        'zdf': '涨跌幅',
        'zdf_d10': '10日涨跌幅',
        'zdf_d20': '20日涨跌幅',
        'zdf_d5': '5日涨跌幅',
        'zdf_d60': '60日涨跌幅',
        'zdf_w52': '52周涨跌幅',
        'zdf_y': '年初至今涨跌幅',
        'zf': '振幅',
        'zljlr': '主力净流入', # (万元)
        'zllc': '主力流出', # (万元)
        'zllc_d5': '5日主力流出', # (万元)
        'zllr': '主力流入', # (万元)
        'zllr_d5': '5日主力流入', # (万元)
        'zsz': '总市值',
        'zxj': '最新价'
    }

    # 重命名列
    df = df.rename(columns=column_mapping)

    return df



########################################
#获取国内各大行业实时数据并写入到历史数据库

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
    #实时行业数据主表
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

        # ==== 历史信息表（cn_industry_hist_daily）写入逻辑 ====
        try:
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
            industry_data_df.loc[:, "amount"] = temp_df["amount"] #成交额
            industry_data_df.loc[:, "amplitude"] = temp_df["amplitude"] #振幅
            industry_data_df.loc[:, "quote_change"] = temp_df["change_rate"] #涨跌幅
            industry_data_df.loc[:, "ups_downs"] = temp_df["ups_downs"] #涨跌额
            industry_data_df.loc[:, "period"] = "daily" #周期
            # print(f'行业历史信息样表：{industry_data_df}')

            try:
                # 生成批量SQL
                sql_batches = industry_sql_batch_generator(
                    table_name='cn_industry_hist_daily',
                    data=industry_data_df,
                    batch_size=1000  # 根据实际情况调整
                )

                # 执行批量插入
                execute_batch_sql(sql_batches)
                print(f"[Success] 行业历史信息表写入完成，数据量：{len(industry_data_df)}")
            except Exception as e:
                print(f"[Critical] 行业历史信息表批量写入失败: {str(e)}")
        except Exception as e:
            print(f"[Error] 行业历史信息表写入失败: {e}")
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
            r = requests.get(url, params=page_params, headers=HEADERS)
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
定义公共函数

"""
def sync_table_structure(table_name: str, data_columns: List[str]):
    """根据实际数据字段动态同步表结构 - 仅在表不存在时创建表并添加字段"""
    try:
        with DBManager.get_new_connection() as conn:
            cursor = conn.cursor()

            # 检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if cursor.fetchone():
                print(f"表 {table_name} 已存在，跳过创建")
                return

            print(f"表 {table_name} 不存在，开始创建...")

            # 创建基础表结构
            create_sql = f"""
                CREATE TABLE `{table_name}` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `date` DATE,
                    `date_int` INT,
                    `code_int` INT,
                    `code` VARCHAR(6),
                    `name` VARCHAR(20),
                    INDEX `idx_date_int` (`date_int`),
                    INDEX `idx_code_int` (`code_int`),
                    UNIQUE INDEX `idx_unique_int` (`code_int`,`date_int`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
            """
            cursor.execute(create_sql)
            print(f"成功创建表 {table_name}")

            # 添加额外字段
            base_columns = {'id', 'date', 'date_int', 'code_int', 'code', 'name'}
            for col in data_columns:
                if col not in base_columns:
                    alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` FLOAT;"
                    cursor.execute(alter_sql)
                    print(f"添加字段 {col} 到表 {table_name}")

            conn.commit()
            print(f"表 {table_name} 结构创建完成")

    except Exception as e:
        print(f"同步表 {table_name} 结构失败：{str(e)}")
        sys.exit(1)


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

def main():

    # 实时股票 OK
    # stock_zh_a_spot_em()
    # 新浪实时股票 K线数据写入 open,close,high,low,volume OK
    stock_hs_a_spot_sina()

    index_dpzs_spot_sina()

    # 实时ETF基金 OK
    # etf_spot_em()

    # 实时指数 OK
    # index_zh_a_spot_em()

    # 实时行业
    industry_zh_a_spot_em()




# main函数入口
if __name__ == '__main__':
    main()

