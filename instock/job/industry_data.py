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
import talib as tl
import time
import datetime 
import mysql.connector
# import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import pandas_market_calendars as mcal
from mysql.connector import Error
from typing import Optional  # 新增导入
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from sqlalchemy.dialects.mysql import TINYINT
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 




TABLE_INDUSTRY_INIT = {
    'name': 'cn_industry_info',
    'cn': '行业初始表',
    'columns': {
        'id' : {'type': INT, 'cn': 'id', 'size': 0, 'en': 'id'},
        'date': {'type': DATE, 'cn': '更新日期', 'size': 90, 'en': 'date'},
        'date_int': {'type': INT, 'cn': '日期_int', 'size': 8, 'en': 'date_int'},
        'name': {'type': VARCHAR(20), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code': {'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'en': 'code'},
        # 'code_int': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code_int'},
        'market_id': {'type': INT, 'cn': '市场标识', 'size': 10, 'en': 'market_id'},
        'code_market': {'type': VARCHAR(20), 'cn': '股票标识', 'size': 120, 'en': 'code_market'}

    }
}

# {"f1":2,"f2":1445.36,"f3":5.38,"f4":73.73,"f8":1.91,"f12":"BK0732","f13":90,"f14":"贵金属","f20":438651456000,"f103":"-","f104":11,"f105":0,"f106":1,"f108":"-","f128":"晓程科技","f140":"300139","f141":0,"f136":11.77,"f152":2,"f207":"玉龙股份","f208":"601028","f209":1,"f222":0.0}
TABLE_INDUSTRY_SPOT = {
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

# 基础字段结构（新增'en'字段）
BASE_COLUMNS = {
                'id' : {'type': INT, 'cn': 'id', 'size': 0,'map': None, 'en': 'id'},
                'date':    {'type': DATE,  'cn': '日期', 'en': 'date', 'map': 0, 'size': 90},
                'date_int':    {'type': INT,  'cn': '日期_int', 'en': 'date_int', 'map': None, 'size': 90},
                'name': {'type': VARCHAR(20), 'cn': '名称', 'size': 120, 'en': 'name'},
                'code': {'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'en': 'code'},
                # 'code_int': {'type': INT, 'cn': '代码', 'size': 70,'map': None, 'en': 'code_int'},
                'open':    {'type': FLOAT, 'cn': '开盘价', 'en': 'open', 'map': 1, 'size': 70},  
                'close':   {'type': FLOAT, 'cn': '收盘价', 'en': 'close', 'map': 2, 'size': 70},   
                'high':    {'type': FLOAT, 'cn': '最高价', 'en': 'high', 'map': 3, 'size': 70}, 
                'low':     {'type': FLOAT, 'cn': '最低价', 'en': 'low', 'map': 4, 'size': 70},    
                'volume':  {'type': FLOAT, 'cn': '成交量', 'en': 'volume', 'map': 5, 'size': 120}, 
                'amount':  {'type': FLOAT, 'cn': '成交额', 'en': 'amount', 'map': 6, 'size': 120},  
                'amplitude': {'type': FLOAT, 'cn': '振幅', 'en': 'amplitude', 'map': 7, 'size': 120},  
                'quote_change': {'type': FLOAT, 'cn': '涨跌幅', 'en': 'quote_change', 'map': 8, 'size': 120},  
                'ups_downs': {'type': FLOAT, 'cn': '涨跌额', 'en': 'ups_downs', 'map': 9, 'size': 120},  
                'turnover': {'type': FLOAT, 'cn': '换手率', 'en': 'turnover', 'map': 10, 'size': 120} ,
                'period': {'type': VARCHAR(20),'cn': '周期','en': 'period', 'size': 120} # 
    }

def create_hist_data(asset_type: str, freq: str, cn_desc: str) -> dict:
    """创建通用历史数据配置
    asset_type: stock/etf/index
    freq: daily/weekly/monthly
    cn_desc: 中文周期描述
    """
    return {
        'name': f'{asset_type}_hist_{freq}',
        'cn': f'{ASSET_CN_MAP[asset_type]}的{cn_desc}行情数据库',
        'columns': BASE_COLUMNS
    }

# 资产类型映射表（新增）
ASSET_CN_MAP = {
    'cn_stock': '股票',
    'cn_etf': 'ETF',
    'cn_index': '指数',
    'cn_industry':'行业'
}

# 股票历史数据配置
CN_INDUSTRY_HIST_DAILY_DATA = create_hist_data('cn_industry', 'daily', '日')
CN_INDUSTRY_HIST_WEEKLY_DATA = create_hist_data('cn_industry', 'weekly', '周')
CN_INDUSTRY_HIST_MONTHLY_DATA = create_hist_data('cn_industry', 'monthly', '月')




TABLE_CN_INDICATORS_FOREIGN_KEY = {'name': 'cn_industry_foreign_key', 'cn': '行业外键',
                              'columns': {
                                       # 'id' : {'type': INT, 'cn': 'id', 'size': 0, 'en': 'id'},
                                        'date': {'type': DATE, 'cn': '日期', 'size': 90},
                                        'date_int': {'type': INT, 'cn': '日期_int', 'size': 90},
                                        'code': {'type': VARCHAR(6), 'cn': '代码', 'size': 70},
                                        # 'code_int': {'type': INT, 'cn': '代码_int', 'size': 70},
                                        'name': {'type': VARCHAR(20), 'cn': '名称', 'size': 70}}}

STOCK_STATS_DATA = {'name': 'calculate_indicator', 'cn': '股票统计/指标计算助手库',
                    'columns': {'close': {'type': FLOAT, 'cn': '收盘价', 'size': 70},
                                'macd': {'type': FLOAT, 'cn': 'dif', 'size': 70},
                                'macds': {'type': FLOAT, 'cn': 'macd', 'size': 70},
                                'macdh': {'type': FLOAT, 'cn': 'histogram', 'size': 70},
                                'kdjk': {'type': FLOAT, 'cn': 'kdjk', 'size': 70},
                                'kdjd': {'type': FLOAT, 'cn': 'kdjd', 'size': 70},
                                'kdjj': {'type': FLOAT, 'cn': 'kdjj', 'size': 70},
                                'boll_ub': {'type': FLOAT, 'cn': 'boll上轨', 'size': 70},
                                'boll': {'type': FLOAT, 'cn': 'boll', 'size': 70},
                                'boll_lb': {'type': FLOAT, 'cn': 'boll下轨', 'size': 70},
                                'trix': {'type': FLOAT, 'cn': 'trix', 'size': 70},
                                'trix_20_sma': {'type': FLOAT, 'cn': 'trma', 'size': 70},
                                'tema': {'type': FLOAT, 'cn': 'tema', 'size': 70},
                                'cr': {'type': FLOAT, 'cn': 'cr', 'size': 70},
                                'cr-ma1': {'type': FLOAT, 'cn': 'cr-ma1', 'size': 70},
                                'cr-ma2': {'type': FLOAT, 'cn': 'cr-ma2', 'size': 70},
                                'cr-ma3': {'type': FLOAT, 'cn': 'cr-ma3', 'size': 70},
                                'rsi_6': {'type': FLOAT, 'cn': 'rsi_6', 'size': 70},
                                'rsi_12': {'type': FLOAT, 'cn': 'rsi_12', 'size': 70},
                                'rsi': {'type': FLOAT, 'cn': 'rsi', 'size': 70},
                                'rsi_24': {'type': FLOAT, 'cn': 'rsi_24', 'size': 70},
                                'vr': {'type': FLOAT, 'cn': 'vr', 'size': 70},
                                'vr_6_sma': {'type': FLOAT, 'cn': 'mavr', 'size': 70},
                                'roc': {'type': FLOAT, 'cn': 'roc', 'size': 70},
                                'rocma': {'type': FLOAT, 'cn': 'rocma', 'size': 70},
                                'rocema': {'type': FLOAT, 'cn': 'rocema', 'size': 70},
                                'pdi': {'type': FLOAT, 'cn': 'pdi', 'size': 70},
                                'mdi': {'type': FLOAT, 'cn': 'mdi', 'size': 70},
                                'dx': {'type': FLOAT, 'cn': 'dx', 'size': 70},
                                'adx': {'type': FLOAT, 'cn': 'adx', 'size': 70},
                                'adxr': {'type': FLOAT, 'cn': 'adxr', 'size': 70},
                                'wr_6': {'type': FLOAT, 'cn': 'wr_6', 'size': 70},
                                'wr_10': {'type': FLOAT, 'cn': 'wr_10', 'size': 70},
                                'wr_14': {'type': FLOAT, 'cn': 'wr_14', 'size': 70},
                                'cci': {'type': FLOAT, 'cn': 'cci', 'size': 70},
                                'cci_84': {'type': FLOAT, 'cn': 'cci_84', 'size': 70},
                                'tr': {'type': FLOAT, 'cn': 'tr', 'size': 70},
                                'atr': {'type': FLOAT, 'cn': 'atr', 'size': 70},
                                'dma': {'type': FLOAT, 'cn': 'dma', 'size': 70},
                                'dma_10_sma': {'type': FLOAT, 'cn': 'ama', 'size': 70},
                                'obv': {'type': FLOAT, 'cn': 'obv', 'size': 70},
                                'sar': {'type': FLOAT, 'cn': 'sar', 'size': 70},
                                'psy': {'type': FLOAT, 'cn': 'psy', 'size': 70},
                                'psyma': {'type': FLOAT, 'cn': 'psyma', 'size': 70},
                                'br': {'type': FLOAT, 'cn': 'br', 'size': 70},
                                'ar': {'type': FLOAT, 'cn': 'ar', 'size': 70},
                                'emv': {'type': FLOAT, 'cn': 'emv', 'size': 70},
                                'emva': {'type': FLOAT, 'cn': 'emva', 'size': 70},
                                'bias': {'type': FLOAT, 'cn': 'bias', 'size': 70},
                                'mfi': {'type': FLOAT, 'cn': 'mfi', 'size': 70},
                                'mfisma': {'type': FLOAT, 'cn': 'mfisma', 'size': 70},
                                'vwma': {'type': FLOAT, 'cn': 'vwma', 'size': 70},
                                'mvwma': {'type': FLOAT, 'cn': 'mvwma', 'size': 70},
                                'ppo': {'type': FLOAT, 'cn': 'ppo', 'size': 70},
                                'ppos': {'type': FLOAT, 'cn': 'ppos', 'size': 70},
                                'ppoh': {'type': FLOAT, 'cn': 'ppoh', 'size': 70},
                                'wt1': {'type': FLOAT, 'cn': 'wt1', 'size': 70},
                                'wt2': {'type': FLOAT, 'cn': 'wt2', 'size': 70},
                                'supertrend_ub': {'type': FLOAT, 'cn': 'supertrend_ub', 'size': 70},
                                'supertrend': {'type': FLOAT, 'cn': 'supertrend', 'size': 70},
                                'supertrend_lb': {'type': FLOAT, 'cn': 'supertrend_lb', 'size': 70},
                                'dpo': {'type': FLOAT, 'cn': 'dpo', 'size': 70},
                                'madpo': {'type': FLOAT, 'cn': 'madpo', 'size': 70},
                                'vhf': {'type': FLOAT, 'cn': 'vhf', 'size': 70},
                                'rvi': {'type': FLOAT, 'cn': 'rvi', 'size': 70},
                                'rvis': {'type': FLOAT, 'cn': 'rvis', 'size': 70},
                                'fi': {'type': FLOAT, 'cn': 'fi', 'size': 70},
                                'force_2': {'type': FLOAT, 'cn': 'force_2', 'size': 70},
                                'force_13': {'type': FLOAT, 'cn': 'force_13', 'size': 70},
                                'ene_ue': {'type': FLOAT, 'cn': 'ene上轨', 'size': 70},
                                'ene': {'type': FLOAT, 'cn': 'ene', 'size': 70},
                                'ene_le': {'type': FLOAT, 'cn': 'ene下轨', 'size': 70},
                                'stochrsi_k': {'type': FLOAT, 'cn': 'stochrsi_k', 'size': 70},
                                'stochrsi_d': {'type': FLOAT, 'cn': 'stochrsi_d', 'size': 70}}}

# 使用字典解包操作来生成 TABLE_CN_STOCK_INDICATORS
TABLE_CN_INDUSTRY_INDICATORS = {
    'name': 'cn_industry_indicators',
    'cn': '股票指标数据',
    'columns': {
        **TABLE_CN_INDICATORS_FOREIGN_KEY['columns'],
        **STOCK_STATS_DATA['columns']
    }
}

RATE_FIELDS_COUNT = 100  # N日收益率字段数目，即N值

# 动态生成 rate_1 到 rate_N 的字段定义
rate_columns = {
    f'rate_{i}': {
        'type': 'FLOAT',
        'cn': f'{i}日收益率',
        'size': 100
    } for i in range(1, RATE_FIELDS_COUNT + 1)
}

TABLE_CN_INDUSTRY_INDICATORS_BUY = {
    'name': 'cn_industry_indicators_buy',
    'cn': '股票指标买入',
    'columns': {
        'id': {'type': 'INT', 'cn': 'id', 'size': 0, 'en': 'id'},
        'date': {'type': 'DATE', 'cn': '日期', 'size': 90},
        'date_int': {'type': 'INT', 'cn': '日期_int', 'size': 90},
        'code': {'type': f'VARCHAR(6)', 'cn': '代码', 'size': 70},
        'code_int': {'type': 'INT', 'cn': '代码_int', 'size': 70},
        'name': {'type': f'VARCHAR(20)', 'cn': '名称', 'size': 70},
        'close': {'type': 'FLOAT', 'cn': '价格', 'size': 70},
        'turnover': {'type': 'FLOAT', 'cn': '换手率', 'size': 70},
        'industry': {'type': f'VARCHAR(20)', 'cn': '所属行业', 'size': 120, 'en': 'industry'},
        # 合并动态生成的 rate 字段
        **rate_columns  # 使用 ** 解包字典
    }
}



# 表注册中心（集中管理所有表结构）
TABLE_REGISTRY = {
    # ----------------------
    # 基础信息表
    # ----------------------
    'cn_industry_info': TABLE_INDUSTRY_INIT,

    # ----------------------
    # 实时行情表
    # ----------------------
    'cn_industry_spot': TABLE_INDUSTRY_SPOT,

    # 行情历史（新增周期维度）
    'cn_industry_hist_daily': CN_INDUSTRY_HIST_DAILY_DATA,
    'cn_industry_hist_weekly': CN_INDUSTRY_HIST_WEEKLY_DATA,
    'cn_industry_hist_monthly': CN_INDUSTRY_HIST_MONTHLY_DATA,

    # ----------------------
    # 指标与回测表
    # ----------------------
    'cn_industry_indicators': TABLE_CN_INDUSTRY_INDICATORS,
    'cn_industry_indicators_buy': TABLE_CN_INDUSTRY_INDICATORS_BUY
    # 'cn_industry_indicators_sell': TABLE_CN_INDUSTRY_INDICATORS_SELL,
    # 'cn_industry_backtest_data': TABLE_CN_INDICATORS_BACKTEST_DATA,


}

# 表映射配置
TABLE_MAP = {
    'industry': {
        'hist_table': CN_INDUSTRY_HIST_DAILY_DATA['name'],
        'info_table': TABLE_INDUSTRY_INIT['name']
    }
}

INDICATOR_TABLES = {
    'industry': 'cn_industry_indicators'
}

# f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f24,f25,f26,f38,f39,f152,f104,f105,f128,f140,f141,f207,f208,f209,f136,f222

# "f140":"002194","f141":0,"f136":10.05,"f152":2,"f207":"*ST九有","f208":"600462","f209":1,"f222":-3.68}



# {"f1":2,"f2":12567.87,"f3":2.6,"f4":318.0,"f5":17401085,"f6":31569248201.0,"f7":3.25,"f8":2.88,"f9":29.17,"f10":1.2,"f11":0.03,"f12":"BK0448","f13":90,"f14":"通信设备","f15":12620.04,"f16":12221.66,"f17":12249.18,"f18":12249.87,"f20":1356301456000,"f21":1117594640000,"f22":0.01,"f24":1.73,"f25":-1.62,"f26":20000104,"f37":"-","f38":71402061824.0,"f39":60358822400.0,"f104":82,"f105":25,"f128":"武汉凡谷","f140":"002194","f141":0,"f136":10.05,"f152":2,"f207":"*ST九有","f208":"600462","f209":1,"f222":-3.68}

########################################
#获取行业实时股票数据数据并写入数据库

def industry_zh_a_spot_em() -> pd.DataFrame:
    '''
    东方财富网-沪深京 A 股-行业实时行情
    https://quote.eastmoney.com/center/gridlist.html#industry_board
    :return: 行业实时行情
    :rtype: pandas.DataFrame
    '''
    #实时行情数据主表
    table_name = TABLE_INDUSTRY_SPOT['name']
    table_name_cols = TABLE_INDUSTRY_SPOT['columns']

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

        # temp_df["date"] = pd.to_datetime("today").strftime("%Y-%m-%d")  # 添加日期字段
        print(f'行业行情数据主表{temp_df}')


        # ==== 主表（cn_industry_spot）写入逻辑 ====
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


        #在temp_df只获取industry_info_cols = TABLE_INDUSTRY_INIT['columns']中配置的字段生成股票基础表

        #股票基础数据表
        industry_info = TABLE_INDUSTRY_INIT['name'] # cn_industry_info
        industry_info_cols = TABLE_INDUSTRY_INIT['columns']

        
        # ==== 基础信息表（cn_industry_info）写入逻辑 ====
        try:
            # 1. 筛选需要的列
            required_columns = [col_info['en'] for col_info in industry_info_cols.values() if 'en' in col_info]
            existing_columns = [col for col in required_columns if col in temp_df.columns]
            if not existing_columns:
                print("[Error] 无有效列可写入基础表")
                return pd.DataFrame()

            industry_info_df = temp_df[existing_columns]

            # 手动设置固定日期（例如 1212-12-12）做索引使用
            fixed_date = "2222-12-22"
            # fixed_date = latest_trade_date
            industry_info_df = temp_df[existing_columns].copy()  # 显式创建独立副本
            industry_info_df.loc[:, "date"] = fixed_date
            industry_info_df.loc[:, "date_int"] = industry_info_df["date"].astype(str).str.replace('-', '')
            if "market_id" in industry_info_df.columns and "code" in industry_info_df.columns:
                industry_info_df.loc[:, "code_market"] = (
                    industry_info_df["market_id"].astype(str) + "." +
                    industry_info_df["code"].astype(str)
                )
            # industry_info_df.loc[:, "industry"] = fixed_date
            print(f"[Success] industry_info_df：{industry_info_df}")
            print(f"[Success] industry_info_df-columns：{industry_info_df.columns}")

            
            # 2. 创建表（如果不存在）
            create_table_if_not_exists(industry_info)
            
            # 3. 同步表结构（动态添加字段）
            conn = DBManager.get_new_connection()
            try:
                同步表结构(conn, industry_info, industry_info_df.columns)
            finally:
                if conn.is_connected():
                    conn.close()
            
            # 4. 生成并执行SQL
            try:
                # 生成批量SQL
                sql_batches = sql_batch_generator(
                    table_name=industry_info,
                    data=industry_info_df,
                    batch_size=1000  # 根据实际情况调整
                )
                
                # 执行批量插入
                execute_batch_sql(sql_batches)
                
                print(f"[Success] 股票基础信息表写入完成，数据量：{len(industry_info_df)}")
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





#获取沪市A股+深市A股行业历史数据并写入数据库
def fetch_all_industry_hist(beg: str = None, end: str = None):
    """获取全量股票历史数据（支持日期区间）"""
    # 处理日期参数
    today = datetime.datetime.now().strftime("%Y%m%d")
    beg = beg or today
    end = end or today

    # 创建表
    for table_config in [
        CN_INDUSTRY_HIST_DAILY_DATA,
        # CN_INDUSTRY_HIST_WEEKLY_DATA,
        # CN_INDUSTRY_HIST_MONTHLY_DATA
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code, market_id, name FROM cn_industry_info WHERE date = (SELECT MAX(date) FROM cn_industry_info WHERE name = '通用设备' ) AND market_id IS NOT NULL", conn)
    conn.close()

    # 新增过滤逻辑：只保留目标股票
    # stock_df = stock_df[stock_df['code'].apply(is_a_stock)]  # <-- 关键修改位置
    print(f"待采集行业数量：{len(stock_df)}")

    # 准备数据容器
    daily_data = pd.DataFrame()
    # weekly_data = pd.DataFrame()
    # monthly_data = pd.DataFrame()

    # 多线程获取数据
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for _, row in stock_df.iterrows():
            code = row['code']
            market_id = row['market_id']
            name = row['name']
            # for period in ["daily", "weekly", "monthly"]:
            for period in ["daily"]:
                futures.append(
                    executor.submit(
                        fetch_single_hist,
                        code, market_id, period, name, "industry", beg, end
                    )
                )

        # 合并数据
        for future in tqdm(as_completed(futures), total=len(futures), desc="获取行业历史数据"):
            data = future.result()
            if data is None:
                continue
            df = data['df']
            if data['period'] == 'daily':
                daily_data = pd.concat([daily_data, df], ignore_index=True)
            # elif data['period'] == 'weekly':
            #     weekly_data = pd.concat([weekly_data, df], ignore_index=True)
            # elif data['period'] == 'monthly':
            #     monthly_data = pd.concat([monthly_data, df], ignore_index=True)


    sync_and_write(CN_INDUSTRY_HIST_DAILY_DATA['name'], daily_data)
    # sync_and_write(CN_INDUSTRY_HIST_WEEKLY_DATA['name'], weekly_data)
    # sync_and_write(CN_INDUSTRY_HIST_MONTHLY_DATA['name'], monthly_data)
    
    # print(f"[Success] 股票历史数据写入完成（日：{len(daily_data)}，周：{len(weekly_data)}，月：{len(monthly_data)}）")
    print(f"[Success] 行业历史数据写入完成（日：{len(daily_data)}）")


def fetch_single_hist(code: str, market_id: str, period: str, name: str, data_type: str, beg: str, end: str):
    """通用函数：获取单个代码的历史数据（股票/ETF/指数）"""
    try:
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

        # 根据数据类型选择配置
        table_config_map = {
            # "stock": CN_STOCK_HIST_DAILY_DATA,
            # "etf": CN_ETF_HIST_DAILY_DATA,
            # "index": CN_INDEX_HIST_DAILY_DATA,
            "industry": CN_INDUSTRY_HIST_DAILY_DATA
        }
        table_config = table_config_map[data_type]

        # 请求参数（ut 统一使用一个值，如原股票/ETF的ut）
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",  # 统一使用股票/ETF的ut
            "klt": {'daily':101, 'weekly':102, 'monthly':103}[period],
            "fqt": 0,
            "secid": f"{market_id}.{code}",
            # "beg": "20200101",
            # "end": datetime.datetime.now().strftime("%Y%m%d"),
            "beg": beg,  # 使用传入的beg
            "end": end,  # 使用传入的end
            "_": int(time.time()*1000)
        }

        # 发送请求与数据处理（与原逻辑一致）
        r = requests.get(url, params=params, timeout=10)
        data_json = r.json()
        if not data_json.get("data"):
            return None

        # 数据处理与字段映射
        df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        en_name = {
            table_config['columns'][k]['map']: table_config['columns'][k]['en']
            for k in table_config['columns']
            if 'map' in table_config['columns'][k]
        }
        
        # 格式转换与字段添加
        numeric_cols = [1,2,3,4,5,6,7,8,9,10]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df.rename(columns=en_name, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df['date_int'] = df['date'].astype(str).str.replace('-', '')
        df['code'] = code
        # df['code_int'] = code
        df['period'] = period
        df['name'] = name
        
        return {'period': period, 'df': df, 'df_columns': df.columns}
    except Exception as e:
        print(f"获取{code} {period}数据失败: {str(e)}")
        return None





def process_3day_data(
    source_table: str = "cn_industry_indicators",
    target_table: str = "industry_3day_indicators",
    sample_code: str = "通用设备"
):
    """
    处理三日指标数据并写入目标表
    :param source_table: 源数据表名
    :param target_table: 目标表名
    :param sample_code: 用于获取最新日期的示例代码
    """
    # 第一步：创建目标表
    create_3day_table("industry_3day_indicators")


    # 第二步：执行分析查询
    query = f"""
    WITH LatestDate AS (
        SELECT MAX(date_int) AS last_date 
        FROM cn_industry_indicators
        WHERE name = "通用设备"
    ),
    3day AS (
        SELECT
            t.date_int,
            t.code,
            t.date,
            t.name,
            t.close,
            t.kdjk,
            LAG(t.kdjk, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS kdjk_day1,
            LAG(t.kdjk, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS kdjk_day2,
            t.kdjd,
            LAG(t.kdjd, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS kdjd_day1,
            LAG(t.kdjd, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS kdjd_day2,
            t.kdjj,
            LAG(t.kdjj, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS kdjj_day1,
            LAG(t.kdjj, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS kdjj_day2,
            t.wr_6,
            LAG(t.wr_6, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS wr_6_day1,
            LAG(t.wr_6, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS wr_6_day2,
            t.wr_10,
            LAG(t.wr_10, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS wr_10_day1,
            LAG(t.wr_10, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS wr_10_day2,
            t.rsi_6,
            LAG(t.rsi_6, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS rsi_6_day1,
            LAG(t.rsi_6, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS rsi_6_day2,
            t.rsi_12,
            LAG(t.rsi_12, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS rsi_12_day1,
            LAG(t.rsi_12, 2) OVER (PARTition BY t.name ORDER BY t.date_int) AS rsi_12_day2,
            t.cci,
            LAG(t.cci, 1) OVER (PARTITION BY t.name ORDER BY t.date_int) AS cci_day1,
            LAG(t.cci, 2) OVER (PARTITION BY t.name ORDER BY t.date_int) AS cci_day2,
            hist.turnover
        FROM cn_industry_indicators t
        JOIN cn_industry_hist_daily hist ON hist.name = t.name AND hist.date_int = t.date_int
        WHERE t.date_int BETWEEN (SELECT last_date - 20 FROM LatestDate) AND (SELECT last_date FROM LatestDate)
    )
    SELECT * FROM 3day
    WHERE kdjk_day2 IS NOT NULL
      AND kdjd_day2 IS NOT NULL
      AND kdjj_day2 IS NOT NULL
      AND wr_6_day2 IS NOT NULL
      AND wr_10_day2 IS NOT NULL
      AND rsi_6_day2 IS NOT NULL
      AND rsi_12_day2 IS NOT NULL
      AND cci_day2 IS NOT NULL;
    """
    
    # 执行查询并获取数据
    conn = DBManager.get_new_connection()
    try:
        df = pd.read_sql(query, conn)
        print(f"从 cn_industry_indicators 获取到 {len(df)} 条三日指标数据")
        
        if not df.empty:
            # 第三步：写入目标表
            sql_batches = sql语句生成器("industry_3day_indicators", df)
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = [executor.submit(DBManager.execute_sql, sql) for sql in sql_batches]
                for future in tqdm(as_completed(futures), total=len(futures), desc="写入进度"):
                    future.result()
            print(f"industry_3day_indicators 数据更新完成，新增 {len(df)} 条记录")
        else:
            print(f"未从 cn_industry_indicators 获取到有效数据")
    except Error as e:
        print(f"处理 cn_industry_indicators 时发生数据库错误: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()



def create_3day_table(table_name: str):
    """创建三日指标表结构"""
    columns = [
        ('id', 'INT AUTO_INCREMENT PRIMARY KEY'),
        ('date', 'DATE'), ('date_int', 'INT'), ('code', 'VARCHAR(6)'), 
        ('name', 'VARCHAR(20)'),('close', 'FLOAT'), 
        ('turnover', 'FLOAT'), 
        ('kdjk', 'FLOAT'), ('kdjk_day1', 'FLOAT'), ('kdjk_day2', 'FLOAT'),
        ('kdjd', 'FLOAT'), ('kdjd_day1', 'FLOAT'), ('kdjd_day2', 'FLOAT'),
        ('kdjj', 'FLOAT'), ('kdjj_day1', 'FLOAT'), ('kdjj_day2', 'FLOAT'),
        ('wr_6', 'FLOAT'), ('wr_6_day1', 'FLOAT'), ('wr_6_day2', 'FLOAT'),
        ('wr_10', 'FLOAT'), ('wr_10_day1', 'FLOAT'), ('wr_10_day2', 'FLOAT'),
        ('rsi_6', 'FLOAT'), ('rsi_6_day1', 'FLOAT'), ('rsi_6_day2', 'FLOAT'),
        ('rsi_12', 'FLOAT'), ('rsi_12_day1', 'FLOAT'), ('rsi_12_day2', 'FLOAT'),
        ('cci', 'FLOAT'), ('cci_day1', 'FLOAT'), ('cci_day2', 'FLOAT')
    ]
    
    # 创建表（使用原生SQL检查表是否存在）
    conn = DBManager.get_new_connection()
    try:
        cursor = conn.cursor()
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone() is not None
        
        if not exists:
            # 创建表
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            create_sql += ",\n".join([f"`{col[0]}` {col[1]}" for col in columns])
            create_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;"
            cursor.execute(create_sql)
            conn.commit()
            print(f"表 {table_name} 创建成功")
            
            # 创建索引（仅在表不存在时创建）
            index_sql = f"""
            CREATE UNIQUE INDEX idx_date_name_int 
            ON {table_name} (date_int, name);
            
            CREATE INDEX idx_name 
            ON {table_name} (name);
            
            CREATE INDEX idx_date_int 
            ON {table_name} (date_int);
            """
            for statement in index_sql.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            conn.commit()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()






def get_latest_names(data_type: str) -> List[str]:
    """获取指定类型的最新代码列表"""
    try:
        with DBManager.get_new_connection() as conn:
            query = f"""
                SELECT name 
                FROM cn_industry_info
                WHERE date = (SELECT MAX(date) FROM cn_industry_info)
            """
            return pd.read_sql(query, conn)['name'].tolist()
    except Exception as e:
        print(f"获取{data_type}代码失败：{str(e)}")
        return []

def get_hist_data(name: str, data_type: str, last_date: str = None) -> pd.DataFrame:
    """获取带日期范围的行情数据"""
    try:
        with DBManager.get_new_connection() as conn:
            base_query = f"""
                SELECT * FROM cn_industry_hist_daily
                WHERE name = '{name}' 
            """
            
            if last_date:
                query = f"""
                    {base_query}
                    AND date >= (
                        SELECT DATE_SUB('{last_date}', INTERVAL {MAX_HISTORY_WINDOW} DAY) 
                        FROM DUAL
                    )
                """
            else:
                query = base_query + " ORDER BY date_int ASC LIMIT 1000"

            data = pd.read_sql(query, conn)
            # --- 调试7: 验证原始数据质量 ---
            # print(f"[DEBUG] {code} 原始数据统计:")
            # print("记录数:", len(data))
            # print("时间范围:", data['date'].min(), "至", data['date'].max())
            # print("缺失值统计:")
            # print(data[['close', 'high', 'low', 'volume']].isnull().sum())
            
            return data.sort_values('date', ascending=True) if not data.empty else pd.DataFrame()
            # return data.sort_values('date', ascending=True)
    except Exception as e:
        print(f"获取{data_type}历史数据失败：{name}-{str(e)}")
        return pd.DataFrame()



def calculate_and_save(name: str, data_type: str):
    """完整的处理流水线"""
    try:
        # 检查目标表是否存在
        table_name = TABLE_CN_INDUSTRY_INDICATORS[data_type]
        # create_table_if_not_exists(table_name)
        
        # 获取最新处理日期
        last_processed_date = get_last_processed_date(table_name, name)
        
        # 获取历史数据（增量逻辑）
        hist_data = get_hist_data(name, data_type, last_processed_date)
        if hist_data.empty:
            print(f"跳过空数据：{data_type} {name}")
            return

        # 检查必需字段是否存在
        required_columns = {'date', 'code', 'close', 'high', 'low', 'volume'}
        missing_columns = required_columns - set(hist_data.columns)
        if missing_columns:
            print(f"数据缺失关键列 {missing_columns}，跳过处理：{name}")
            return

        # 计算指标
        # print(f"\n=== 开始处理 {code} ===")
        indicators = calculate_indicators(hist_data)
        
        # --- 调试5: 输出前5行数据样本 ---
        # print(f"[DEBUG] {code} 计算结果样本:")
        # print(indicators.head())
        
        # --- 调试6: 检查是否存在负无穷或零值 ---
        # print(f"[DEBUG] {code} 异常值统计:")
        # print("Inf values:", (indicators == np.inf).sum().sum())
        # print("-Inf values:", (indicators == -np.inf).sum().sum())
        # print("Zero values:", (indicators == 0).sum().sum())

        # 过滤已存在数据
        if last_processed_date:
            indicators = indicators[indicators['date'] > last_processed_date]
        
        # 写入数据库前检查数据是否为空
        if not indicators.empty:
            # 新增过滤条件：删除 cci_84 为0的行
            if 'cci_84' in indicators.columns:
                indicators = indicators[indicators['cci_84'] != 0]
            if not indicators.empty:
                sync_and_save(table_name, indicators)
                print(f"更新{data_type}指标：{code} {len(indicators)}条")
        else:
            print(f"无新数据需更新：{code}")
    except Exception as e:
        print(f"处理{data_type} {code}失败：{str(e)}")

def get_latest_codes(data_type: str) -> List[int]:
    """获取指定类型的最新代码列表（返回整数列表）"""
    try:
        with DBManager.get_new_connection() as conn:
            query = f"""
                SELECT name 
                FROM cn_industry_info
                WHERE date = (SELECT MAX(date) FROM cn_industry_info)
            """
            df = pd.read_sql(query, conn)
            # return df['name'].astype(int).tolist()  # 强制转换为整数列表
            return df['name']
    except Exception as e:
        print(f"获取{data_type}代码失败：{str(e)}")
        return []

def get_last_processed_date(table: str, name: str) -> str:
    """获取指定代码的最后处理日期"""
    try:
        with DBManager.get_new_connection() as conn:
            query = f"""
                SELECT MAX(date) AS last_date 
                FROM {table} 
                WHERE name = '{name}'
            """
            result = pd.read_sql(query, conn)
            return result.iloc[0]['last_date']
    except:
        return None


def get_last_processed_dates_batch(table: str, names: List[str]) -> Dict[str, str]:  # 修正返回类型
    """批量获取多个代码的最后处理日期"""
    if not names:
    # if names.empty:
        return {}

    try:
        with DBManager.get_new_connection() as conn:
            # 添加单引号并使用正确变量名
            name_list = ",".join([f"'{str(name)}'" for name in names])
            
            query = f"""
                SELECT name, MAX(date) AS last_date 
                FROM {table}  # 安全警告：需验证table参数合法性
                WHERE name IN ({name_list})
                GROUP BY name  # 修正分组字段（原code_int疑似错误）
            """
            df = pd.read_sql(query, conn)
            return df.set_index('name')['last_date'].astype(str).to_dict()
    except Exception as e:
        print(f"获取最后处理日期失败：{str(e)}")
        return {}

def get_hist_data_batch(batch_names: List[str], data_type: str) -> pd.DataFrame:
    """严格按批次执行单次查询（无分块）"""
    if batch_names.empty:  # 修正空列表检查
        return pd.DataFrame()

    try:
        with DBManager.get_new_connection() as conn:
            # 为每个名称添加单引号并拼接
            name_list = ",".join([f"'{str(name)}'" for name in batch_names])
            
            query = f"""
                SELECT * 
                FROM cn_industry_hist_daily
                WHERE name IN ({name_list})
                ORDER BY name, date_int DESC
            """
            return pd.read_sql(query, conn)
    except Exception as e:
        print(f"获取批次数据失败：{str(e)}")
        return pd.DataFrame()

def sync_and_save(table_name: str, data: pd.DataFrame):
    # print(f"[DEBUG] 准备写入数据，形状：{data.shape}")
    """同步表结构并保存数据"""
    # with DBManager.get_new_connection() as conn:
    #     try:
    #         同步表结构(conn, table_name, data.columns)
    #     finally:
    #         if conn.is_connected():
    #             conn.close()
                
    sql_txt = sql语句生成器(table_name, data)
    execute_raw_sql(sql_txt)

def calculate_indicators(data):
    # 检查数据长度是否满足最小窗口（例如MACD需要至少34条数据）
    min_window = 34  # 根据TA-Lib指标要求调整
    if len(data) < min_window:
        print(f"[WARNING] 数据不足{min_window}条，无法计算指标")
        return pd.DataFrame()

    daily_data_indicators = pd.DataFrame()
    data = data.sort_values(by='date', ascending=True)
   # 原始字段
    daily_data_indicators['date'] = data['date']
    daily_data_indicators['code'] = data['code']


    # 格式化日期为 YYYYMMDD
    daily_data_indicators['date_int'] = data['date'].astype(str).str.replace('-', '')

    # if 'code_int' in data.columns:
    #     daily_data_indicators['code_int'] = data['code_int']
    if 'name' in data.columns:
        daily_data_indicators['name'] = data['name']
    daily_data_indicators['close'] = data['close']
    '''
    计算ETF数据的各种指标。

    参数:
    data (pd.DataFrame): 包含ETF每日数据的DataFrame，至少包含'close', 'high', 'low', 'volume'列 收盘价，最高价，最低价，成交量

    返回:
    pd.DataFrame: 包含计算后指标数据的DataFrame
    '''


    # 计算MACD
    daily_data_indicators['macd'], daily_data_indicators['macds'], daily_data_indicators['macdh'] = tl.MACD(data['close'])

    # 计算KDJ的K和D
    daily_data_indicators['kdjk'], daily_data_indicators['kdjd'] = tl.STOCH(
        data['high'], 
        data['low'], 
        data['close'],
        fastk_period=9,    # 默认参数需显式指定
        slowk_period=5,
        slowk_matype=1,
        slowd_period=5,
        slowd_matype=1
    )

    # 手动计算J线（J = 3*K - 2*D）
    daily_data_indicators['kdjj'] = 3 * daily_data_indicators['kdjk'] - 2 * daily_data_indicators['kdjd']

    # 计算BOLL
    daily_data_indicators['boll_ub'], daily_data_indicators['boll'], daily_data_indicators['boll_lb'] = tl.BBANDS(data['close'])
    
    # 计算W&R
    daily_data_indicators['wr_6'] = tl.WILLR(data['high'], data['low'], data['close'], timeperiod=6)
    daily_data_indicators['wr_10'] = tl.WILLR(data['high'], data['low'], data['close'], timeperiod=10)
    daily_data_indicators['wr_14'] = tl.WILLR(data['high'], data['low'], data['close'], timeperiod=14)

    # 计算CCI
    daily_data_indicators['cci'] = tl.CCI(data['high'], data['low'], data['close'])
    daily_data_indicators['cci_84'] = tl.SMA(daily_data_indicators['cci'], timeperiod=84)

    # # 计算TRIX和TRMA（假设TRMA是TRIX的简单移动平均）
    # daily_data_indicators['trix'] = tl.TRIX(data['close'])
    # daily_data_indicators['trix_20_sma'] = tl.SMA(daily_data_indicators['trix'], timeperiod=20)

    # 计算CR
    # data['m_price'] = data['amount'] / data['volume']
    # data['m_price_sf1'] = data['m_price'].shift(1, fill_value=0.0)
    # data['h_m'] = data['high'] - data[['m_price_sf1', 'high']].min(axis=1)
    # data['m_l'] = data['m_price_sf1'] - data[['m_price_sf1', 'low']].min(axis=1)
    # data['h_m_sum'] = data['h_m'].rolling(window=26).sum()
    # data['m_l_sum'] = data['m_l'].rolling(window=26).sum()
    # data['cr'] = (data['h_m_sum'] / data['m_l_sum']).fillna(0).replace([np.inf, -np.inf], 0) * 100
    # data['cr-ma1'] = data['cr'].rolling(window=5).mean()
    # data['cr-ma2'] = data['cr'].rolling(window=10).mean()
    # data['cr-ma3'] = data['cr'].rolling(window=20).mean()

    # 计算SMA（简单移动平均）
    # data['sma'] = tl.SMA(data['close'])

    # 计算RSI
    daily_data_indicators['rsi_6'] = tl.RSI(data['close'], timeperiod=6)
    daily_data_indicators['rsi_12'] = tl.RSI(data['close'], timeperiod=12)
    daily_data_indicators['rsi'] = tl.RSI(data['close'])
    daily_data_indicators['rsi_24'] = tl.RSI(data['close'], timeperiod=24)

    # 手动计算VR
    close_diff = data['close'].diff()
    up_volume = data['volume'] * (close_diff > 0).astype(int)
    down_volume = data['volume'] * (close_diff < 0).astype(int)
    daily_data_indicators['vr'] = up_volume.rolling(window=26).sum() / down_volume.rolling(window=26).sum() * 100
    daily_data_indicators['vr'] = daily_data_indicators['vr'].fillna(0.0).replace([np.inf, -np.inf], 0.0)
    daily_data_indicators['vr_6_sma'] = tl.SMA(daily_data_indicators['vr'], timeperiod=6)

    # 计算ROC
    # 修正：ROC函数返回值只有一个，原代码可能有误
    daily_data_indicators['roc'] = tl.ROC(data['close'])

     # 计算DMI相关指标
    timeperiod = 14
    daily_data_indicators['pdi'] = tl.PLUS_DI(data['high'], data['low'], data['close'], timeperiod=timeperiod)
    daily_data_indicators['mdi'] = tl.MINUS_DI(data['high'], data['low'], data['close'], timeperiod=timeperiod)
    daily_data_indicators['dx'] = tl.DX(data['high'], data['low'], data['close'], timeperiod=timeperiod)
    daily_data_indicators['adx'] = tl.ADX(data['high'], data['low'], data['close'], timeperiod=timeperiod)
    daily_data_indicators['adxr'] = tl.ADXR(data['high'], data['low'], data['close'], timeperiod=timeperiod)


    # 计算TR和ATR
    daily_data_indicators['tr'] = tl.TRANGE(data['high'], data['low'], data['close'])
    daily_data_indicators['atr'] = tl.ATR(data['high'], data['low'], data['close'])

    # 计算DMA和AMA（假设AMA是DMA的简单移动平均）
    # data['dma'] = tl.DMA(data['close'])
    # data['dma_10_sma'] = tl.SMA(data['dma'], timeperiod=10)

    # 计算OBV
    daily_data_indicators['obv'] = tl.OBV(data['close'], data['volume'])

    # 计算SAR
    daily_data_indicators['sar'] = tl.SAR(data['high'], data['low'])

    # 计算PSY
    price_up = (data['close'] > data['close'].shift(1)).astype(int)
    daily_data_indicators['psy'] = (price_up.rolling(12).sum() / 12 * 100).fillna(0)
    daily_data_indicators['psyma'] = daily_data_indicators['psy'].rolling(6).mean()

    # 计算BRAR
    prev_close = data['close'].shift(1, fill_value=0)
    br_up = (data['high'] - prev_close).clip(lower=0)
    br_down = (prev_close - data['low']).clip(lower=0)
    daily_data_indicators['br'] = (br_up.rolling(26).sum() / br_down.rolling(26).sum()).fillna(0).replace([np.inf, -np.inf], 0) * 100

    ar_up = (data['high'] - data['open']).clip(lower=0)
    ar_down = (data['open'] - data['low']).clip(lower=0)
    daily_data_indicators['ar'] = (ar_up.rolling(26).sum() / ar_down.rolling(26).sum()).fillna(0).replace([np.inf, -np.inf], 0) * 100

    # 计算EMV
    hl_avg = (data['high'] + data['low']) / 2
    prev_hl_avg = hl_avg.shift(1, fill_value=0)
    volume = data['volume'].replace(0, 1)  # 避免除零
    daily_data_indicators['emv'] = ((hl_avg - prev_hl_avg) * (data['high'] - data['low']) / volume).rolling(14).sum()
    daily_data_indicators['emva'] = daily_data_indicators['emv'].rolling(9).mean()

    # # 计算BIAS
    # daily_data_indicators['bias'] = (data['close'] - tl.SMA(data['close'], timeperiod=6)) / tl.SMA(data['close'], timeperiod=6) * 100
    # daily_data_indicators['bias_12'] = (data['close'] - tl.SMA(data['close'], timeperiod=12)) / tl.SMA(data['close'], timeperiod=12) * 100
    # daily_data_indicators['bias_24'] = (data['close'] - tl.SMA(data['close'], timeperiod=24)) / tl.SMA(data['close'], timeperiod=24) * 100

    # 计算MFI
    daily_data_indicators['mfi'] = tl.MFI(data['high'], data['low'], data['close'], data['volume'])
    daily_data_indicators['mfisma'] = tl.SMA(daily_data_indicators['mfi'])

    # 计算VWMA
    daily_data_indicators['vwma'] = (data['close'] * data['volume']).cumsum() / data['volume'].cumsum()
    daily_data_indicators['mvwma'] = tl.SMA(daily_data_indicators['vwma'])

    # 计算PPO
    daily_data_indicators['ppo'] = tl.PPO(data['close'], fastperiod=12, slowperiod=26, matype=1)
    daily_data_indicators['ppos'] = tl.EMA(daily_data_indicators['ppo'], timeperiod=9)
    daily_data_indicators['ppoh'] = daily_data_indicators['ppo'] - daily_data_indicators['ppos']
    daily_data_indicators['ppo'] = daily_data_indicators['ppo'].fillna(0)
    daily_data_indicators['ppos'] = daily_data_indicators['ppos'].fillna(0)
    daily_data_indicators['ppoh'] = daily_data_indicators['ppoh'].fillna(0)

    # 计算WT（假设WT1和WT2的计算方法）
    daily_data_indicators['wt1'] = (data['close'] - tl.SMA(data['close'], timeperiod=10)) / tl.STDDEV(data['close'], timeperiod=10)
    daily_data_indicators['wt2'] = (data['close'] - tl.SMA(data['close'], timeperiod=20)) / tl.STDDEV(data['close'], timeperiod=20)

    # 计算Supertrend（简单示例，实际可能需要更复杂的实现）
    # atr_multiplier = 3
    # daily_data_indicators['atr'] = tl.ATR(data['high'], data['low'], data['close'])
    # daily_data_indicators['upper_band'] = data['close'] + (atr_multiplier * daily_data_indicators['atr'])
    # daily_data_indicators['lower_band'] = data['close'] - (atr_multiplier * daily_data_indicators['atr'])
    # daily_data_indicators['supertrend'] = daily_data_indicators['upper_band']
    # daily_data_indicators['supertrend_ub'] = daily_data_indicators['upper_band']
    # daily_data_indicators['supertrend_lb'] = daily_data_indicators['lower_band']

    # 计算DPO
    daily_data_indicators['dpo'] = data['close'] - tl.SMA(data['close'], timeperiod=20)
    daily_data_indicators['madpo'] = tl.SMA(daily_data_indicators['dpo'])

    # 计算VHF
    window = 28
    high_close = data['close'].rolling(window).max()
    low_close = data['close'].rolling(window).min()
    sum_diff = abs(data['close'] - data['close'].shift(1)).rolling(window).sum()
    daily_data_indicators['vhf'] = ((high_close - low_close) / sum_diff).fillna(0)

    # 计算RVI
    rvi_x = (
        (data['close'] - data['open']) +
        2 * (data['close'].shift(1) - data['open'].shift(1)) +
        2 * (data['close'].shift(2) - data['open'].shift(2)) +
        (data['close'].shift(3) - data['open'].shift(3))
    ) / 6

    rvi_y = (
        (data['high'] - data['low']) +
        2 * (data['high'].shift(1) - data['low'].shift(1)) +
        2 * (data['high'].shift(2) - data['low'].shift(2)) +
        (data['high'].shift(3) - data['low'].shift(3))
    ) / 6

    daily_data_indicators['rvi'] = (rvi_x.rolling(10).mean() / rvi_y.rolling(10).mean()).fillna(0)
    daily_data_indicators['rvis'] = (
        daily_data_indicators['rvi'] + 
        2 * daily_data_indicators['rvi'].shift(1) + 
        2 * daily_data_indicators['rvi'].shift(2) + 
        daily_data_indicators['rvi'].shift(3)
    ) / 6

    # 计算FI
    daily_data_indicators['fi'] = (data['close'] - data['close'].shift(1)) * data['volume']
    daily_data_indicators['force_2'] = tl.SMA(daily_data_indicators['fi'], timeperiod=2)
    daily_data_indicators['force_13'] = tl.SMA(daily_data_indicators['fi'], timeperiod=13)

    # 计算ENE
    daily_data_indicators['ene_ue'] = tl.EMA(data['close'], timeperiod=25) + 2 * tl.STDDEV(data['close'], timeperiod=25)
    daily_data_indicators['ene'] = tl.EMA(data['close'], timeperiod=25)
    daily_data_indicators['ene_le'] = tl.EMA(data['close'], timeperiod=25) - 2 * tl.STDDEV(data['close'], timeperiod=25)

    # 计算STOCHRSI
    daily_data_indicators['stochrsi_k'], daily_data_indicators['stochrsi_d'] = tl.STOCHRSI(data['close'])
    daily_data_indicators = daily_data_indicators.replace([np.inf, -np.inf], np.nan)
    daily_data_indicators = daily_data_indicators.fillna(0)

    # print(f'{daily_data_indicators}')

    return daily_data_indicators




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
    create_index("idx_date_name_int", ["date_int", "name"], is_unique=True)
    create_index("idx_name", ["name"])
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
        table_config = TABLE_REGISTRY.get(table_name, {})
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
                        sql_type = _get_sql_type(col_info['type'])
                        
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
    # if 'code' in data.columns and 'code_int' not in data.columns:
    #     data.insert(0, 'code_int', data['code'].astype(int))

    # SQL模板（批量版本）
    sql_template = """INSERT INTO `{table_name}` ({columns}) 
        VALUES {values}
        ON DUPLICATE KEY UPDATE {update_clause};"""

    # 定义字段和更新子句
    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'name']
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




def execute_raw_sql(sql, conn=None, params=None):
    """执行原始SQL（支持传入现有连接）"""
    close_conn = False
    if conn is None:
        conn = DBManager.get_new_connection()
        close_conn = True
    if not conn:
        return False

    try:
        cursor = conn.cursor(buffered=True)
        if isinstance(sql, list):
            sql = ';'.join(sql)  # 将列表元素拼接成字符串
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        for statement in statements:
            cursor.execute(statement)
            while True:
                if cursor.with_rows:
                    cursor.fetchall()
                if not cursor.nextset():
                    break
        conn.commit()
        return True
    except Error as e:
        print(f"执行失败: {e}")
        conn.rollback()
        return False
    finally:
        if close_conn and conn.is_connected():
            cursor.close()
            conn.close()


def is_open(price):
    return not np.isnan(price)

def _get_sql_type(py_type):
    """将 SQLAlchemy 类型转换为数据库原生类型字符串（增强版本）"""
    # 处理实例类型（如 VARCHAR(20)）
    if isinstance(py_type, VARCHAR):
        return f"VARCHAR({py_type.length})"
    elif isinstance(py_type, (DATE, DATETIME)):
        return py_type.__class__.__name__.upper()
    elif isinstance(py_type, FLOAT):
        return "FLOAT"
    elif isinstance(py_type, BIGINT):
        return "BIGINT"
    elif isinstance(py_type, INT):
        return "INT"
    elif isinstance(py_type, TINYINT):
        # return "TINYINT"
        return "BOOLEAN"  # 直接映射为 MySQL 的 BOOLEAN 类型

    # 处理类类型（如 DATE 类）
    if py_type == DATE:
        return "DATE"
    elif py_type == DATETIME:
        return "DATETIME"
    elif py_type == FLOAT:
        return "FLOAT"
    elif py_type == BIGINT:
        return "BIGINT"
    elif py_type == INT:
        return "INT"
    elif py_type == TINYINT:
        return "BOOLEAN"  # 直接映射为 MySQL 的 BOOLEAN 类型
        # return "TINYINT"

    raise ValueError(f"Unsupported type: {py_type}")

def sync_and_write(table_name: str, data: pd.DataFrame):
    """同步表结构并写入数据（新增索引优化）"""
    if data.empty:
        print(f"[Warning] 表 {table_name} 无数据可写入")
        return

    conn = None
    try:
        conn = DBManager.get_new_connection()
        cursor = conn.cursor()

        # 禁用索引
        disable_sql = f"ALTER TABLE `{table_name}` DISABLE KEYS;"
        cursor.execute(disable_sql)
        conn.commit()

        # 同步表结构
        同步表结构(conn, table_name, data.columns)

        # 生成批量SQL列表
        sql_batches = sql_batch_generator(table_name, data)  # 返回的是列表

        # 逐条执行SQL
        for sql in sql_batches:
            if not execute_raw_sql(sql, conn):  # 传入现有连接
                raise Exception(f"{table_name} 写入失败")

        # 启用索引
        enable_sql = f"ALTER TABLE `{table_name}` ENABLE KEYS;"
        cursor.execute(enable_sql)
        conn.commit()

    except Exception as e:
        print(f"[Critical] 数据写入异常: {str(e)}")
        if conn and conn.is_connected():
            cursor.execute(f"ALTER TABLE `{table_name}` ENABLE KEYS;")
            conn.commit()
        raise
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()



def check_if_first_run() -> bool:
    """检查是否为首次运行（所有指标表无数据或表不存在）"""
    table_name = "cn_industry_indicators"
    try:
        with DBManager.get_new_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{table_name}';")
            exists = cursor.fetchone() is not None
            if exists:
                # 表存在，检查是否有数据
                query = f"SELECT 1 FROM `{table_name}` LIMIT 1"
                result = pd.read_sql(query, conn)
                if not result.empty:
                    return False  # 存在数据，非首次运行
            else:
                # 表不存在，属于首次运行
                return True
    except Exception as e:
        print(f"检查表 {table_name} 失败：{str(e)}")
        return True



def sync_table_structure(table_name: str, data_columns: List[str]):
    """根据实际数据字段动态同步表结构"""
    try:
        with DBManager.get_new_connection() as conn:
            cursor = conn.cursor()
            
            # 1. 创建表（如果不存在）
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                # 基础表结构（date, code_int, code, name）
                create_sql = f"""
                    CREATE TABLE `{table_name}` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                        `date` DATE,
                        `date_int` INT,
                        `code` VARCHAR(6),
                        `name` VARCHAR(20)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
                """
                cursor.execute(create_sql)
                print(f"创建基础表 {table_name}")

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
            create_index("idx_date_name_int", ["date_int", "name"], is_unique=True)
            create_index("idx_name", ["name"])
            create_index("idx_date_int", ["date_int"])

            # 2. 动态添加指标字段
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}' 
                  AND TABLE_SCHEMA = DATABASE()
            """)
            existing_columns = {row[0] for row in cursor.fetchall()}
            
            # 3. 遍历指标字段，添加缺失列
            for col in data_columns:
                if col not in existing_columns and col not in ['id', 'date','date_int', 'code', 'name']:
                    # 自动推断字段类型（假设均为FLOAT）
                    alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` FLOAT;"
                    cursor.execute(alter_sql)
                    print(f"动态添加字段 {col} 到表 {table_name}")
            conn.commit()
    except Exception as e:
        print(f"同步表 {table_name} 结构失败：{str(e)}")
        sys.exit(1)


def process_single_code(    
    name: str, 
    data_type: str, 
    code_data: pd.DataFrame,
    last_processed_date: Optional[str] = None
) -> pd.DataFrame:
    """处理单个代码的计算逻辑（完全基于传入的code_data）"""
    try:
        # 检查数据量是否足够（34条为TA-Lib最低要求）
        if len(code_data) < 34:
            print(f"代码 {name} 数据不足34条（实际{len(code_data)}条），跳过")
            return pd.DataFrame()

        # 计算指标
        indicators = calculate_indicators(code_data)
        if indicators.empty:
            return pd.DataFrame()

        # 过滤已处理日期
        # 使用预取的last_processed_date过滤数据
        if last_processed_date:
            indicators = indicators[indicators['date_int'] > last_processed_date]


        # 过滤无效cci_84
        if 'cci_84' in indicators.columns:
            indicators = indicators[indicators['cci_84'] != 0]

        return indicators if not indicators.empty else pd.DataFrame()
    except Exception as e:
        print(f"处理代码 {name} 失败：{str(e)}")
        return pd.DataFrame()



def main():
    # 行业历史数据
    fetch_all_industry_hist()

    # 实时行业 OK
    industry_zh_a_spot_em()


    # 检查是否为首次运行（任一指标表无数据）
    is_first_run = check_if_first_run()

    # 首次运行时动态同步表结构
    if is_first_run:
        # 定义每个类型的示例code_int
        sample_codes = {
            'industry': '通用设备'     # 假设name='通用设备'为有效股票
        }
        
        for data_type in ['industry']:
            table_name = "cn_industry_indicators"
            name = sample_codes[data_type]

            # create_table_if_not_exists(table_name)  # 确保只执行一次
            
            # 1. 获取足够的历史数据（至少34条）
            # 获取历史数据（直接传递整数）
            hist_data = get_hist_data(name, data_type, last_date=None)
            if len(hist_data) < 34:
                print(f"错误：{data_type}示例数据不足34条（当前{len(hist_data)}条），无法同步结构！")
                sys.exit(1)
                
            # 2. 计算指标，获取所有字段
            indicators = calculate_indicators(hist_data)
            if indicators.empty:
                print(f"错误：{data_type}指标计算失败！")
                sys.exit(1)
                
            # 3. 动态同步表结构（基于实际字段）
            sync_table_structure(table_name, indicators.columns)
            
        print("首次运行表结构同步完成")


       

    batch_size = 200
    max_workers = 6
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for data_type in ['industry']:
            names = get_latest_codes(data_type)
            print(f"开始处理 {data_type} 共 {len(names)} 个代码")

            for batch_idx in range(0, len(names), batch_size):
                batch_names = names[batch_idx:batch_idx + batch_size]
                print(f"处理批次 {batch_idx//batch_size+1}，代码数：{len(batch_names)}")

                # 1. 获取本批次历史数据
                batch_data = get_hist_data_batch(batch_names, data_type)
                if batch_data.empty:
                    print(f"批次 {batch_idx//batch_size+1} 无数据，跳过")
                    continue

                # 2. 批量获取最后处理日期（关键修改点）
                # 从本批数据中提取所有唯一代码
                unique_codes_in_batch = batch_data['name'].unique().tolist()
                last_dates_map = get_last_processed_dates_batch(
                    INDICATOR_TABLES[data_type], 
                    unique_codes_in_batch
                )

                # 3. 并行处理本批次代码
                futures = []
                for name in batch_names:
                    # 从批次数据中提取单个代码数据
                    code_data = batch_data[batch_data['name'] == name].copy()
                    if code_data.empty:
                        continue
                    # 提交任务时传入预取的最后处理日期
                    futures.append(executor.submit(
                        process_single_code,
                        name=name,
                        data_type=data_type,
                        code_data=code_data,
                        last_processed_date=last_dates_map.get(name, None)
                    ))

                # 4. 合并并提交本批次结果
                valid_dfs = []
                for future in as_completed(futures):
                    df = future.result()
                    if df is not None and not df.empty:
                        valid_dfs.append(df)
                
                if valid_dfs:
                    combined_data = pd.concat(valid_dfs, ignore_index=True)
                    sync_and_save(INDICATOR_TABLES[data_type], combined_data)
                    print(f"批次提交成功，记录数：{len(combined_data)}")
                else:
                    print(f"本批次无有效数据")


    process_3day_data()


# main函数入口
if __name__ == '__main__':
    main()