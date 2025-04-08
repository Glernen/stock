#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, INT, DATETIME
from sqlalchemy.dialects.mysql import BIT
import talib as tl
from instock.core.strategy import enter
from instock.core.strategy import turtle_trade
from instock.core.strategy import climax_limitdown
from instock.core.strategy import low_atr
from instock.core.strategy import backtrace_ma250
from instock.core.strategy import breakthrough_platform
from instock.core.strategy import parking_apron
from instock.core.strategy import low_backtrace_increase
from instock.core.strategy import keep_increasing
from instock.core.strategy import high_tight_flag
# from sqlalchemy.sql.sqltypes import DATE, VARCHAR, FLOAT, BIGINT, DATETIME, INT

__author__ = 'myh '
__date__ = '2023/3/10 '

RATE_FIELDS_COUNT = 100  # N日收益率字段数目，即N值
_COLLATE = "utf8mb4_general_ci"




TABLE_STOCK_INIT = {
    'name': 'cn_stock_info',
    'cn': '股票初始表',
    'columns': {
        # 'date': {'type': DATE, 'cn': '更新日期', 'size': 0, 'en': 'date'},
        'name': {'type': VARCHAR(20, collation=_COLLATE), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code'},
        'code_id': {'type': INT, 'cn': '市场标识', 'size': 10, 'en': 'code_id'}
    }
}


TABLE_ETF_INIT = {
    'name': 'cn_etf_info',
    'cn': '基金初始表',
    'columns': {
        # 'date': {'type': DATE, 'cn': '更新日期', 'size': 0, 'en': 'date'},
        'name': {'type': VARCHAR(20, collation=_COLLATE), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code'},
        'code_id': {'type': INT, 'cn': '市场标识', 'size': 10, 'en': 'code_id'}
    }
}


TABLE_INDEX_INIT = {
    'name': 'cn_index_info',
    'cn': '指数初始表',
    'columns': {
        # 'date': {'type': DATE, 'cn': '更新日期', 'size': 0, 'en': 'date'},
        'name': {'type': VARCHAR(20, collation=_COLLATE), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code': {'type': INT, 'cn': '代码_int', 'size': 10, 'en': 'code'},
        'code_id': {'type': INT, 'cn': '市场标识', 'size': 10, 'en': 'code_id'}
    }
}


TABLE_CN_STOCK_ATTENTION = {'name': 'cn_stock_attention', 'cn': '我的关注',
                            'columns': {'datetime': {'type': DATETIME, 'cn': '日期', 'size': 0, 'en': 'datetime'},
                            'code': {'type': INT, 'cn': '代码', 'size': 10, 'en': 'code'}}}

TABLE_CN_ETF_SPOT = {
    'name': 'cn_etf_spot',
    'cn': '每日ETF数据',
    'columns': {
        'date': {'map': None, 'type': DATE, 'cn': '日期', 'size': 0, 'en': 'date'},
        'code': {'map': 'f12',  'type': VARCHAR(6, _COLLATE), 'cn': '代码', 'size': 60, 'en': 'code'},
        'name': {'map': 'f14', 'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code_id':{'map': 'f13', 'type': INT, 'cn': '市场标识', 'size': 10,  'en': 'code_id'},
        'new_price': {'map': 'f2', 'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
        'change_rate': {'map': 'f3',  'type': FLOAT, 'cn': '涨跌幅', 'size': 70, 'en': 'change_rate'},
        'ups_downs': {'map': 'f4',  'type': FLOAT, 'cn': '涨跌额', 'size': 70, 'en': 'ups_downs'},
        'volume': {'map': 'f5',  'type': BIGINT, 'cn': '成交量', 'size': 90, 'en': 'volume'},
        'deal_amount': {'map': 'f6',  'type': BIGINT, 'cn': '成交额', 'size': 100, 'en': 'deal_amount'},
        'open_price': {'map': 'f17',  'type': FLOAT, 'cn': '开盘价', 'size': 70, 'en': 'open_price'},
        'high_price': {'map': 'f15',  'type': FLOAT, 'cn': '最高价', 'size': 70, 'en': 'high_price'},
        'low_price': {'map': 'f16',  'type': FLOAT, 'cn': '最低价', 'size': 70, 'en': 'low_price'},
        'pre_close_price': {'map': 'f18', 'type': FLOAT, 'cn': '昨收', 'size': 70, 'en': 'pre_close_price'},
        'turnoverrate': {'map': 'f8', 'type': FLOAT, 'cn': '换手率', 'size': 70, 'en': 'turnoverrate'},
        'total_market_cap': {'map': 'f20', 'type': BIGINT, 'cn': '总市值', 'size': 120, 'en': 'total_market_cap'},
        'free_cap': {'map': 'f21', 'type': BIGINT, 'cn': '流通市值', 'size': 120, 'en': 'free_cap'}
    }
}

TABLE_CN_INDEX_SPOT = {
    'name': 'cn_index_spot',
    'cn': '每日指数数据',
    'columns': {
        'date': {'map': None, 'type': DATE, 'cn': '日期', 'size': 0, 'en': 'date'},
        'code': {'map': 'f12',  'type': VARCHAR(6, _COLLATE), 'cn': '代码', 'size': 60, 'en': 'code'},
        'name': {'map': 'f14', 'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 120, 'en': 'name'},
        'code_id':{'map': 'f13', 'type': INT, 'cn': '市场标识', 'size': 10,  'en': 'code_id'},
        'new_price': {'map': 'f2', 'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
        'change_rate': {'map': 'f3',  'type': FLOAT, 'cn': '涨跌幅', 'size': 70, 'en': 'change_rate'},
        'ups_downs': {'map': 'f4',  'type': FLOAT, 'cn': '涨跌额', 'size': 70, 'en': 'ups_downs'},
        'volume': {'map': 'f5',  'type': BIGINT, 'cn': '成交量', 'size': 90, 'en': 'volume'},
        'deal_amount': {'map': 'f6',  'type': BIGINT, 'cn': '成交额', 'size': 100, 'en': 'deal_amount'},
        'open_price': {'map': 'f17',  'type': FLOAT, 'cn': '开盘价', 'size': 70, 'en': 'open_price'},
        'high_price': {'map': 'f15',  'type': FLOAT, 'cn': '最高价', 'size': 70, 'en': 'high_price'},
        'low_price': {'map': 'f16',  'type': FLOAT, 'cn': '最低价', 'size': 70, 'en': 'low_price'},
        'pre_close_price': {'map': 'f18', 'type': FLOAT, 'cn': '昨收', 'size': 70, 'en': 'pre_close_price'},
        'turnoverrate': {'map': 'f8', 'type': FLOAT, 'cn': '换手率', 'size': 70, 'en': 'turnoverrate'},
        'total_market_cap': {'map': 'f20', 'type': BIGINT, 'cn': '总市值', 'size': 120, 'en': 'total_market_cap'},
        'free_cap': {'map': 'f21', 'type': BIGINT, 'cn': '流通市值', 'size': 120, 'en': 'free_cap'}
    }
}

TABLE_CN_STOCK_SPOT = {
    'name': 'cn_stock_spot',
    'cn': '每日股票数据',
    'columns': {
        'date': {'type': DATE, 'cn': '日期', 'size': 0, 'map': None, 'en': 'date'},
        'code': {'type': INT, 'cn': '代码', 'size': 10, 'map': 'f12', 'en': 'code'},
        'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'map': 'f14', 'en': 'name'},
        'code_id':{'type': INT, 'cn': '市场标识', 'size': 10, 'map': 'f13', 'en': 'code_id'},
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
        'industry': {'type': VARCHAR(20, _COLLATE), 'cn': '所处行业', 'size': 100, 'map': 'f100', 'en': 'industry'},
        'listing_date': {'type': DATE, 'cn': '上市时间', 'size': 110, 'map': 'f26', 'en': 'listing_date'}
    }
}



TABLE_CN_STOCK_SPOT_BUY = {'name': 'cn_stock_spot_buy', 'cn': '基本面选股',
                           'columns': TABLE_CN_STOCK_SPOT['columns'].copy()}

CN_STOCK_FUND_FLOW = ({'name': 'stock_individual_fund_flow_rank', 'cn': '今日',
                       'columns': {'code': {'type': INT, 'cn': '代码', 'size': 10, 'en': 'code'},
                                   'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
                                   'change_rate': {'type': FLOAT, 'cn': '今日涨跌幅', 'size': 70, 'en': 'change_rate'},
                                   'fund_amount': {'type': BIGINT, 'cn': '今日主力净流入-净额', 'size': 100, 'en': 'fund_amount'},
                                   'fund_rate': {'type': FLOAT, 'cn': '今日主力净流入-净占比', 'size': 70, 'en': 'fund_rate'},
                                   'fund_amount_super': {'type': BIGINT, 'cn': '今日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super'},
                                   'fund_rate_super': {'type': FLOAT, 'cn': '今日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super'},
                                   'fund_amount_large': {'type': BIGINT, 'cn': '今日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large'},
                                   'fund_rate_large': {'type': FLOAT, 'cn': '今日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large'},
                                   'fund_amount_medium': {'type': BIGINT, 'cn': '今日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium'},
                                   'fund_rate_medium': {'type': FLOAT, 'cn': '今日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium'},
                                   'fund_amount_small': {'type': BIGINT, 'cn': '今日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small'},
                                   'fund_rate_small': {'type': FLOAT, 'cn': '今日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small'}}},
                      {'name': 'stock_individual_fund_flow_rank', 'cn': '3日',
                       'columns': {'code': {'type': INT, 'cn': '代码', 'size': 10, 'en': 'code'},
                                   'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
                                   'change_rate_3': {'type': FLOAT, 'cn': '3日涨跌幅', 'size': 70, 'en': 'change_rate_3'},
                                   'fund_amount_3': {'type': BIGINT, 'cn': '3日主力净流入-净额', 'size': 100, 'en': 'fund_amount_3'},
                                   'fund_rate_3': {'type': FLOAT, 'cn': '3日主力净流入-净占比', 'size': 70, 'en': 'fund_rate_3'},
                                   'fund_amount_super_3': {'type': BIGINT, 'cn': '3日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super_3'},
                                   'fund_rate_super_3': {'type': FLOAT, 'cn': '3日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super_3'},
                                   'fund_amount_large_3': {'type': BIGINT, 'cn': '3日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large_3'},
                                   'fund_rate_large_3': {'type': FLOAT, 'cn': '3日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large_3'},
                                   'fund_amount_medium_3': {'type': BIGINT, 'cn': '3日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium_3'},
                                   'fund_rate_medium_3': {'type': FLOAT, 'cn': '3日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium_3'},
                                   'fund_amount_small_3': {'type': BIGINT, 'cn': '3日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small_3'},
                                   'fund_rate_small_3': {'type': FLOAT, 'cn': '3日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small_3'}}},
                      {'name': 'stock_individual_fund_flow_rank', 'cn': '5日',
                       'columns': {'code': {'type': INT, 'cn': '代码', 'size': 10, 'en': 'code'},
                                   'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
                                   'change_rate_5': {'type': FLOAT, 'cn': '5日涨跌幅', 'size': 70, 'en': 'change_rate_5'},
                                   'fund_amount_5': {'type': BIGINT, 'cn': '5日主力净流入-净额', 'size': 100, 'en': 'fund_amount_5'},
                                   'fund_rate_5': {'type': FLOAT, 'cn': '5日主力净流入-净占比', 'size': 70, 'en': 'fund_rate_5'},
                                   'fund_amount_super_5': {'type': BIGINT, 'cn': '5日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super_5'},
                                   'fund_rate_super_5': {'type': FLOAT, 'cn': '5日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super_5'},
                                   'fund_amount_large_5': {'type': BIGINT, 'cn': '5日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large_5'},
                                   'fund_rate_large_5': {'type': FLOAT, 'cn': '5日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large_5'},
                                   'fund_amount_medium_5': {'type': BIGINT, 'cn': '5日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium_5'},
                                   'fund_rate_medium_5': {'type': FLOAT, 'cn': '5日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium_5'},
                                   'fund_amount_small_5': {'type': BIGINT, 'cn': '5日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small_5'},
                                   'fund_rate_small_5': {'type': FLOAT, 'cn': '5日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small_5'}}},
                      {'name': 'stock_individual_fund_flow_rank', 'cn': '10日',
                       'columns': {'code': {'type': INT, 'cn': '代码', 'size': 10, 'en': 'code'},
                                   'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                   'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70, 'en': 'new_price'},
                                   'change_rate_10': {'type': FLOAT, 'cn': '10日涨跌幅', 'size': 70, 'en': 'change_rate_10'},
                                   'fund_amount_10': {'type': BIGINT, 'cn': '10日主力净流入-净额', 'size': 100, 'en': 'fund_amount_10'},
                                   'fund_rate_10': {'type': FLOAT, 'cn': '10日主力净流入-净占比', 'size': 70, 'en': 'fund_rate_10'},
                                   'fund_amount_super_10': {'type': BIGINT, 'cn': '10日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super_10'},
                                   'fund_rate_super_10': {'type': FLOAT, 'cn': '10日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super_10'},
                                   'fund_amount_large_10': {'type': BIGINT, 'cn': '10日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large_10'},
                                   'fund_rate_large_10': {'type': FLOAT, 'cn': '10日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large_10'},
                                   'fund_amount_medium_10': {'type': BIGINT, 'cn': '10日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium_10'},
                                   'fund_rate_medium_10': {'type': FLOAT, 'cn': '10日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium_10'},
                                   'fund_amount_small_10': {'type': BIGINT, 'cn': '10日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small_10'},
                                   'fund_rate_small_10': {'type': FLOAT, 'cn': '10日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small_10'}}})

TABLE_CN_STOCK_FUND_FLOW = {'name': 'cn_stock_fund_flow', 'cn': '股票资金流向',
                            'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0, 'en': 'date'}}}
for cf in CN_STOCK_FUND_FLOW:
    TABLE_CN_STOCK_FUND_FLOW['columns'].update(cf['columns'].copy())

CN_STOCK_SECTOR_FUND_FLOW = (('行业资金流', '概念资金流'),
                             ({'name': 'stock_sector_fund_flow_rank', 'cn': '今日',
                              'columns': {'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                          'change_rate': {'type': FLOAT, 'cn': '今日涨跌幅', 'size': 70, 'en': 'change_rate'},
                                          'fund_amount': {'type': BIGINT, 'cn': '今日主力净流入-净额', 'size': 100, 'en': 'fund_amount'},
                                          'fund_rate': {'type': FLOAT, 'cn': '今日主力净流入-净占比', 'size': 70, 'en': 'fund_rate'},
                                          'fund_amount_super': {'type': BIGINT, 'cn': '今日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super'},
                                          'fund_rate_super': {'type': FLOAT, 'cn': '今日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super'},
                                          'fund_amount_large': {'type': BIGINT, 'cn': '今日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large'},
                                          'fund_rate_large': {'type': FLOAT, 'cn': '今日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large'},
                                          'fund_amount_medium': {'type': BIGINT, 'cn': '今日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium'},
                                          'fund_rate_medium': {'type': FLOAT, 'cn': '今日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium'},
                                          'fund_amount_small': {'type': BIGINT, 'cn': '今日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small'},
                                          'fund_rate_small': {'type': FLOAT, 'cn': '今日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small'},
                                          'stock_name': {'type': VARCHAR(20, _COLLATE), 'cn': '今日主力净流入最大股', 'size': 70, 'en': 'stock_name'}}},
                             {'name': 'stock_individual_fund_flow_rank', 'cn': '5日',
                              'columns': {'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                          'change_rate_5': {'type': FLOAT, 'cn': '5日涨跌幅', 'size': 70, 'en': 'change_rate_5'},
                                          'fund_amount_5': {'type': BIGINT, 'cn': '5日主力净流入-净额', 'size': 100, 'en': 'fund_amount_5'},
                                          'fund_rate_5': {'type': FLOAT, 'cn': '5日主力净流入-净占比', 'size': 70, 'en': 'fund_rate_5'},
                                          'fund_amount_super_5': {'type': BIGINT, 'cn': '5日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super_5'},
                                          'fund_rate_super_5': {'type': FLOAT, 'cn': '5日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super_5'},
                                          'fund_amount_large_5': {'type': BIGINT, 'cn': '5日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large_5'},
                                          'fund_rate_large_5': {'type': FLOAT, 'cn': '5日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large_5'},
                                          'fund_amount_medium_5': {'type': BIGINT, 'cn': '5日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium_5'},
                                          'fund_rate_medium_5': {'type': FLOAT, 'cn': '5日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium_5'},
                                          'fund_amount_small_5': {'type': BIGINT, 'cn': '5日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small_5'},
                                          'fund_rate_small_5': {'type': FLOAT, 'cn': '5日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small_5'},
                                          'stock_name_5': {'type': VARCHAR(20, _COLLATE), 'cn': '5日主力净流入最大股', 'size': 70, 'en': 'stock_name_5'}}},
                             {'name': 'stock_individual_fund_flow_rank', 'cn': '10日',
                              'columns': {'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                          'change_rate_10': {'type': FLOAT, 'cn': '10日涨跌幅', 'size': 70, 'en': 'change_rate_10'},
                                          'fund_amount_10': {'type': BIGINT, 'cn': '10日主力净流入-净额', 'size': 100, 'en': 'fund_amount_10'},
                                          'fund_rate_10': {'type': FLOAT, 'cn': '10日主力净流入-净占比', 'size': 70, 'en': 'fund_rate_10'},
                                          'fund_amount_super_10': {'type': BIGINT, 'cn': '10日超大单净流入-净额', 'size': 100, 'en': 'fund_amount_super_10'},
                                          'fund_rate_super_10': {'type': FLOAT, 'cn': '10日超大单净流入-净占比', 'size': 70, 'en': 'fund_rate_super_10'},
                                          'fund_amount_large_10': {'type': BIGINT, 'cn': '10日大单净流入-净额', 'size': 100, 'en': 'fund_amount_large_10'},
                                          'fund_rate_large_10': {'type': FLOAT, 'cn': '10日大单净流入-净占比', 'size': 70, 'en': 'fund_rate_large_10'},
                                          'fund_amount_medium_10': {'type': BIGINT, 'cn': '10日中单净流入-净额', 'size': 100, 'en': 'fund_amount_medium_10'},
                                          'fund_rate_medium_10': {'type': FLOAT, 'cn': '10日中单净流入-净占比', 'size': 70, 'en': 'fund_rate_medium_10'},
                                          'fund_amount_small_10': {'type': BIGINT, 'cn': '10日小单净流入-净额', 'size': 100, 'en': 'fund_amount_small_10'},
                                          'fund_rate_small_10': {'type': FLOAT, 'cn': '10日小单净流入-净占比', 'size': 70, 'en': 'fund_rate_small_10'},
                                          'stock_name_10': {'type': VARCHAR(20, _COLLATE), 'cn': '10日主力净流入最大股', 'size': 70, 'en': 'stock_name_10'}}}))

TABLE_CN_STOCK_FUND_FLOW_INDUSTRY = {'name': 'cn_stock_fund_flow_industry', 'cn': '行业资金流向',
                                     'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0, 'en': 'date'}}}
for cf in CN_STOCK_SECTOR_FUND_FLOW[1]:
    TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns'].update(cf['columns'].copy())

TABLE_CN_STOCK_FUND_FLOW_CONCEPT = {'name': 'cn_stock_fund_flow_concept', 'cn': '概念资金流向',
                                    'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0, 'en': 'date'}}}
for cf in CN_STOCK_SECTOR_FUND_FLOW[1]:
    TABLE_CN_STOCK_FUND_FLOW_CONCEPT['columns'].update(cf['columns'].copy())

TABLE_CN_STOCK_BONUS = {'name': 'cn_stock_bonus', 'cn': '股票分红配送',
                        'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0, 'en': 'date'},
                                    'code': {'type': INT, 'cn': '代码', 'size': 10, 'en': 'code'},
                                    'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70, 'en': 'name'},
                                    'convertible_total_rate': {'type': FLOAT, 'cn': '送转股份-送转总比例', 'size': 70, 'en': 'convertible_total_rate'},
                                    'convertible_rate': {'type': FLOAT, 'cn': '送转股份-送转比例', 'size': 70, 'en': 'convertible_rate'},
                                    'convertible_transfer_rate': {'type': FLOAT, 'cn': '送转股份-转股比例', 'size': 70, 'en': 'convertible_transfer_rate'},
                                    'bonusaward_rate': {'type': FLOAT, 'cn': '现金分红-现金分红比例', 'size': 70, 'en': 'bonusaward_rate'},
                                    'bonusaward_yield': {'type': FLOAT, 'cn': '现金分红-股息率', 'size': 70, 'en': 'bonusaward_yield'},
                                    'basic_eps': {'type': FLOAT, 'cn': '每股收益', 'size': 70, 'en': 'basic_eps'},
                                    'bvps': {'type': FLOAT, 'cn': '每股净资产', 'size': 70, 'en': 'bvps'},
                                    'per_capital_reserve': {'type': FLOAT, 'cn': '每股公积金', 'size': 70, 'en': 'per_capital_reserve'},
                                    'per_unassign_profit': {'type': FLOAT, 'cn': '每股未分配利润', 'size': 70, 'en': 'per_unassign_profit'},
                                    'netprofit_yoy_ratio': {'type': FLOAT, 'cn': '净利润同比增长', 'size': 70, 'en': 'netprofit_yoy_ratio'},
                                    'total_shares': {'type': BIGINT, 'cn': '总股本', 'size': 120, 'en': 'total_shares'},
                                    'plan_date': {'type': DATE, 'cn': '预案公告日', 'size': 110, 'en': 'plan_date'},
                                    'record_date': {'type': DATE, 'cn': '股权登记日', 'size': 110, 'en': 'record_date'},
                                    'ex_dividend_date': {'type': DATE, 'cn': '除权除息日', 'size': 110, 'en': 'ex_dividend_date'},
                                    'progress': {'type': VARCHAR(50, _COLLATE), 'cn': '方案进度', 'size': 100, 'en': 'progress'},
                                    'report_date': {'type': DATE, 'cn': '最新公告日期', 'size': 110, 'en': 'report_date'}}}

TABLE_CN_STOCK_TOP = {'name': 'cn_stock_top', 'cn': '股票龙虎榜',
                      'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                  'code': {'type': INT, 'cn': '代码', 'size': 10},
                                  'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70},
                                  'ranking_times': {'type': FLOAT, 'cn': '上榜次数', 'size': 70},
                                  'sum_buy': {'type': FLOAT, 'cn': '累积购买额', 'size': 100},
                                  'sum_sell': {'type': FLOAT, 'cn': '累积卖出额', 'size': 100},
                                  'net_amount': {'type': FLOAT, 'cn': '净额', 'size': 100},
                                  'buy_seat': {'type': FLOAT, 'cn': '买入席位数', 'size': 100},
                                  'sell_seat': {'type': FLOAT, 'cn': '卖出席位数', 'size': 100}}}

TABLE_CN_STOCK_BLOCKTRADE = {'name': 'cn_stock_blocktrade', 'cn': '股票大宗交易',
                             'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                         'code': {'type': INT, 'cn': '代码', 'size': 10},
                                         'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70},
                                         'new_price': {'type': FLOAT, 'cn': '收盘价', 'size': 70},
                                         'change_rate': {'type': FLOAT, 'cn': '涨跌幅', 'size': 70},
                                         'average_price': {'type': FLOAT, 'cn': '成交均价', 'size': 70},
                                         'overflow_rate': {'type': FLOAT, 'cn': '折溢率', 'size': 120},
                                         'trade_number': {'type': FLOAT, 'cn': '成交笔数', 'size': 70},
                                         'sum_volume': {'type': FLOAT, 'cn': '成交总量', 'size': 100},
                                         'sum_turnover': {'type': FLOAT, 'cn': '成交总额', 'size': 100},
                                         'turnover_market_rate': {'type': FLOAT, 'cn': '成交占比流通市值',
                                                                  'size': 120}}}

# 基础字段结构（新增'en'字段）
BASE_COLUMNS = {
                'date':    {'type': DATE,  'cn': '日期', 'en': 'date', 'map': 0, 'size': 70}, 
                'name': {'type': VARCHAR(20, collation=_COLLATE), 'cn': '名称', 'size': 120, 'en': 'name'},
                'code': {'type': INT, 'cn': '代码_int', 'size': 0, 'en': 'code'},
                'code_str': {'type': INT, 'cn': '代码', 'size': 70, 'en': 'code'},
                'open':    {'type': FLOAT, 'cn': '开盘价', 'en': 'open', 'map': 1, 'size': 70},  # 对应 open_price 的 map: f17
                'close':   {'type': FLOAT, 'cn': '收盘价', 'en': 'close', 'map': 2, 'size': 70},   # 对应 new_price 的 map: f2
                'high':    {'type': FLOAT, 'cn': '最高价', 'en': 'high', 'map': 3, 'size': 70},  # 对应 high_price 的 map: f15
                'low':     {'type': FLOAT, 'cn': '最低价', 'en': 'low', 'map': 4, 'size': 70},    # 对应 low_price 的 map: f16
                'volume':  {'type': FLOAT, 'cn': '成交量', 'en': 'volume', 'map': 5, 'size': 120},  # 对应 volume 的 map: f5
                'amount':  {'type': FLOAT, 'cn': '成交额', 'en': 'amount', 'map': 6, 'size': 120},  # 对应 deal_amount 的 map: f6
                'amplitude': {'type': FLOAT, 'cn': '振幅', 'en': 'amplitude', 'map': 7, 'size': 120},  # 对应 amplitude 的 map: f7
                'quote_change': {'type': FLOAT, 'cn': '涨跌幅', 'en': 'quote_change', 'map': 8, 'size': 120},  # 对应 change_rate 的 map: f3
                'ups_downs': {'type': FLOAT, 'cn': '涨跌额', 'en': 'ups_downs', 'map': 9, 'size': 120},  # 对应 ups_downs 的 map: f4
                'turnover': {'type': FLOAT, 'cn': '换手率', 'en': 'turnover', 'map': 10, 'size': 120} ,
                'period': {'type': VARCHAR(20, _COLLATE),'cn': '周期','en': 'period', 'size': 120} # 对应 turnoverrate 的 map: f8
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
    'cn_index': '指数'
}

# 股票历史数据配置
CN_STOCK_HIST_DAILY_DATA = create_hist_data('cn_stock', 'daily', '日')
CN_STOCK_HIST_WEEKLY_DATA = create_hist_data('cn_stock', 'weekly', '周')
CN_STOCK_HIST_MONTHLY_DATA = create_hist_data('cn_stock', 'monthly', '月')


# ETF历史数据配置
CN_ETF_HIST_DAILY_DATA = create_hist_data('cn_etf', 'daily', '日')  # 注意变量名修正
CN_ETF_HIST_WEEKLY_DATA = create_hist_data('cn_etf', 'weekly', '周')
CN_ETF_HIST_MONTHLY_DATA = create_hist_data('cn_etf', 'monthly', '月')

# 指数历史数据配置
CN_INDEX_HIST_DAILY_DATA = create_hist_data('cn_index', 'daily', '日')
CN_INDEX_HIST_WEEKLY_DATA = create_hist_data('cn_index', 'weekly', '周')
CN_INDEX_HIST_MONTHLY_DATA = create_hist_data('cn_index', 'monthly', '月')


CN_STOCK_HIST_DATA = {'name': 'fund_etf_hist_em', 'cn': '基金某时间段的日行情数据库',
                      'columns': {'date': {'type': DATE, 'cn': '日期'},
                                  'open': {'type': FLOAT, 'cn': '开盘'},
                                  'close': {'type': FLOAT, 'cn': '收盘'},
                                  'high': {'type': FLOAT, 'cn': '最高'},
                                  'low': {'type': FLOAT, 'cn': '最低'},
                                  'volume': {'type': FLOAT, 'cn': '成交量'},
                                  'amount': {'type': FLOAT, 'cn': '成交额'},
                                  'amplitude': {'type': FLOAT, 'cn': '振幅'},
                                  'quote_change': {'type': FLOAT, 'cn': '涨跌幅'},
                                  'ups_downs': {'type': FLOAT, 'cn': '涨跌额'},
                                  'turnover': {'type': FLOAT, 'cn': '换手率'}}}

TABLE_CN_STOCK_BACKTEST_DATA = {'name': 'cn_stock_backtest_data', 'cn': '股票回归测试数据',
                                'columns': {'rate_%s' % i: {'type': FLOAT, 'cn': '%s日收益率' % i, 'size': 100} for i in
                                            range(1, RATE_FIELDS_COUNT + 1, 1)}}

TABLE_CN_ETF_BACKTEST_DATA = {'name': 'cn_etf_backtest_data', 'cn': 'ETF回归测试数据',
                                'columns': {'rate_%s' % i: {'type': FLOAT, 'cn': '%s日收益率' % i, 'size': 100} for i in
                                            range(1, RATE_FIELDS_COUNT + 1, 1)}}

TABLE_CN_INDEX_BACKTEST_DATA = {'name': 'cn_index_backtest_data', 'cn': '指数回归测试数据',
                                'columns': {'rate_%s' % i: {'type': FLOAT, 'cn': '%s日收益率' % i, 'size': 100} for i in
                                            range(1, RATE_FIELDS_COUNT + 1, 1)}}

STOCK_STATS_DATA = {'name': 'calculate_indicator', 'cn': '股票统计/指标计算助手库',
                    'columns': {'close': {'type': FLOAT, 'cn': '价格', 'size': 0},
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

TABLE_CN_STOCK_FOREIGN_KEY = {'name': 'cn_stock_foreign_key', 'cn': '股票外键',
                              'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                          'code': {'type': INT, 'cn': '代码', 'size': 10},
                                          'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70},
                                          'code_str': {'type': INT, 'cn': '代码', 'size': 70, 'en': 'code'}}}

TABLE_CN_STOCK_INDICATORS = {'name': 'cn_stock_indicators', 'cn': '股票指标数据',
                             'columns': TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()}
TABLE_CN_STOCK_INDICATORS['columns'].update(STOCK_STATS_DATA['columns'])

_tmp_columns = TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()
_tmp_columns.update(TABLE_CN_STOCK_BACKTEST_DATA['columns'])

TABLE_CN_STOCK_INDICATORS_BUY = {'name': 'cn_stock_indicators_buy', 'cn': '股票指标买入',
                                 'columns': _tmp_columns}

TABLE_CN_STOCK_INDICATORS_SELL = {'name': 'cn_stock_indicators_sell', 'cn': '股票指标卖出',
                                  'columns': _tmp_columns}

# ETF数据表

TABLE_CN_ETF_FOREIGN_KEY = {'name': 'cn_etf_foreign_key', 'cn': '基金外键',
                              'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                          'code': {'type': INT, 'cn': '代码', 'size': 10},
                                          'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70}}} 

TABLE_CN_ETF_INDICATORS = {'name': 'cn_etf_indicators', 'cn': 'ETF指标数据',
                             'columns': TABLE_CN_ETF_FOREIGN_KEY['columns'].copy()}
TABLE_CN_ETF_INDICATORS['columns'].update(STOCK_STATS_DATA['columns'])

__tmp_columns = TABLE_CN_ETF_FOREIGN_KEY['columns'].copy()
__tmp_columns.update(TABLE_CN_ETF_BACKTEST_DATA['columns'])

TABLE_CN_ETF_INDICATORS_BUY = {'name': 'cn_etf_indicators_buy', 'cn': 'ETF指标买入',
                                 'columns': __tmp_columns}

TABLE_CN_ETF_INDICATORS_SELL = {'name': 'cn_etf_indicators_sell', 'cn': 'ETF指标卖出',
                                  'columns': __tmp_columns}

# 指数数据表
 
TABLE_CN_INDEX_FOREIGN_KEY = {'name': 'cn_index_foreign_key', 'cn': '指数外键',
                              'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0},
                                          'code': {'type': INT, 'cn': '代码', 'size': 10},
                                          'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70}}} 
INDEX_STATS_DATA = {'name': 'calculate_index_indicator', 'cn': '指标计算助手库',
                    'columns': {'close': {'type': FLOAT, 'cn': '价格', 'size': 0},
                                'macd': {'type': FLOAT, 'cn': 'dif', 'size': 70},
                                'macds': {'type': FLOAT, 'cn': 'macd', 'size': 70},
                                'macdh': {'type': FLOAT, 'cn': 'histogram', 'size': 70},
                                'kdjk': {'type': FLOAT, 'cn': 'kdjk', 'size': 70},
                                'kdjd': {'type': FLOAT, 'cn': 'kdjd', 'size': 70},
                                'kdjj': {'type': FLOAT, 'cn': 'kdjj', 'size': 70},
                                'boll_ub': {'type': FLOAT, 'cn': 'boll上轨', 'size': 70},
                                'boll': {'type': FLOAT, 'cn': 'boll', 'size': 70},
                                'boll_lb': {'type': FLOAT, 'cn': 'boll下轨', 'size': 70},
                                'cr-ma1': {'type': FLOAT, 'cn': 'cr-ma1', 'size': 70},
                                'cr-ma2': {'type': FLOAT, 'cn': 'cr-ma2', 'size': 70},
                                'cr-ma3': {'type': FLOAT, 'cn': 'cr-ma3', 'size': 70},
                                'rsi_6': {'type': FLOAT, 'cn': 'rsi_6', 'size': 70},
                                'rsi_12': {'type': FLOAT, 'cn': 'rsi_12', 'size': 70},
                                'rsi': {'type': FLOAT, 'cn': 'rsi', 'size': 70},
                                'rsi_24': {'type': FLOAT, 'cn': 'rsi_24', 'size': 70},
                                'wr_6': {'type': FLOAT, 'cn': 'wr_6', 'size': 70},
                                'wr_10': {'type': FLOAT, 'cn': 'wr_10', 'size': 70},
                                'wr_14': {'type': FLOAT, 'cn': 'wr_14', 'size': 70},
                                'cci': {'type': FLOAT, 'cn': 'cci', 'size': 70},
                                'cci_84': {'type': FLOAT, 'cn': 'cci_84', 'size': 70}}}

TABLE_CN_INDEX_INDICATORS = {'name': 'cn_index_indicators', 'cn': '指数指标数据',
                             'columns': TABLE_CN_INDEX_FOREIGN_KEY['columns'].copy()}
TABLE_CN_INDEX_INDICATORS['columns'].update(STOCK_STATS_DATA['columns'])

___tmp_columns = TABLE_CN_INDEX_FOREIGN_KEY['columns'].copy()
___tmp_columns.update(TABLE_CN_INDEX_BACKTEST_DATA['columns'])

TABLE_CN_INDEX_INDICATORS_BUY = {'name': 'cn_index_indicators_buy', 'cn': '指数指标买入',
                                 'columns': ___tmp_columns}

TABLE_CN_INDEX_INDICATORS_SELL = {'name': 'cn_index_indicators_sell', 'cn': '指数指标卖出',
                                  'columns': ___tmp_columns}



TABLE_CN_STOCK_STRATEGIES = [
    {'name': 'cn_stock_strategy_enter', 'cn': '放量上涨', 'size': 70, 'func': enter.check_volume,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_keep_increasing', 'cn': '均线多头', 'size': 70, 'func': keep_increasing.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_parking_apron', 'cn': '停机坪', 'size': 70, 'func': parking_apron.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_backtrace_ma250', 'cn': '回踩年线', 'size': 70, 'func': backtrace_ma250.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_breakthrough_platform', 'cn': '突破平台', 'size': 70,
     'func': breakthrough_platform.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_low_backtrace_increase', 'cn': '无大幅回撤', 'size': 70,
     'func': low_backtrace_increase.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_turtle_trade', 'cn': '海龟交易法则', 'size': 70, 'func': turtle_trade.check_enter,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_high_tight_flag', 'cn': '高而窄的旗形', 'size': 70,
     'func': high_tight_flag.check_high_tight,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_climax_limitdown', 'cn': '放量跌停', 'size': 70, 'func': climax_limitdown.check,
     'columns': _tmp_columns},
    {'name': 'cn_stock_strategy_low_atr', 'cn': '低ATR成长', 'size': 70, 'func': low_atr.check_low_increase,
     'columns': _tmp_columns}
]

STOCK_KLINE_PATTERN_DATA = {'name': 'cn_stock_pattern_recognitions', 'cn': 'K线形态',
                            'columns': {
                                'tow_crows': {'type': INT, 'cn': '两只乌鸦', 'size': 70, 'func': tl.CDL2CROWS},
                                'upside_gap_two_crows': {'type': INT, 'cn': '向上跳空的两只乌鸦', 'size': 70,
                                                         'func': tl.CDLUPSIDEGAP2CROWS},
                                'three_black_crows': {'type': INT, 'cn': '三只乌鸦', 'size': 70,
                                                      'func': tl.CDL3BLACKCROWS},
                                'identical_three_crows': {'type': INT, 'cn': '三胞胎乌鸦', 'size': 70,
                                                          'func': tl.CDLIDENTICAL3CROWS},
                                'three_line_strike': {'type': INT, 'cn': '三线打击', 'size': 70,
                                                      'func': tl.CDL3LINESTRIKE},
                                'dark_cloud_cover': {'type': INT, 'cn': '乌云压顶', 'size': 70,
                                                     'func': tl.CDLDARKCLOUDCOVER},
                                'evening_doji_star': {'type': INT, 'cn': '十字暮星', 'size': 70,
                                                      'func': tl.CDLEVENINGDOJISTAR},
                                'doji_Star': {'type': INT, 'cn': '十字星', 'size': 70, 'func': tl.CDLDOJISTAR},
                                'hanging_man': {'type': INT, 'cn': '上吊线', 'size': 70,
                                                'func': tl.CDLHANGINGMAN},
                                'hikkake_pattern': {'type': INT, 'cn': '陷阱', 'size': 70,
                                                    'func': tl.CDLHIKKAKE},
                                'modified_hikkake_pattern': {'type': INT, 'cn': '修正陷阱', 'size': 70,
                                                             'func': tl.CDLHIKKAKEMOD},
                                'in_neck_pattern': {'type': INT, 'cn': '颈内线', 'size': 70,
                                                    'func': tl.CDLINNECK},
                                'on_neck_pattern': {'type': INT, 'cn': '颈上线', 'size': 70,
                                                    'func': tl.CDLONNECK},
                                'thrusting_pattern': {'type': INT, 'cn': '插入', 'size': 70,
                                                      'func': tl.CDLTHRUSTING},
                                'shooting_star': {'type': INT, 'cn': '射击之星', 'size': 70,
                                                  'func': tl.CDLSHOOTINGSTAR},
                                'stalled_pattern': {'type': INT, 'cn': '停顿形态', 'size': 70,
                                                    'func': tl.CDLSTALLEDPATTERN},
                                'advance_block': {'type': INT, 'cn': '大敌当前', 'size': 70,
                                                  'func': tl.CDLADVANCEBLOCK},
                                'high_wave_candle': {'type': INT, 'cn': '风高浪大线', 'size': 70,
                                                     'func': tl.CDLHIGHWAVE},
                                'engulfing_pattern': {'type': INT, 'cn': '吞噬模式', 'size': 70,
                                                      'func': tl.CDLENGULFING},
                                'abandoned_baby': {'type': INT, 'cn': '弃婴', 'size': 70,
                                                   'func': tl.CDLABANDONEDBABY},
                                'closing_marubozu': {'type': INT, 'cn': '收盘缺影线', 'size': 70,
                                                     'func': tl.CDLCLOSINGMARUBOZU},
                                'doji': {'type': INT, 'cn': '十字', 'size': 70, 'func': tl.CDLDOJI},
                                'up_down_gap': {'type': INT, 'cn': '向上/下跳空并列阳线', 'size': 70,
                                                'func': tl.CDLGAPSIDESIDEWHITE},
                                'long_legged_doji': {'type': INT, 'cn': '长脚十字', 'size': 70,
                                                     'func': tl.CDLLONGLEGGEDDOJI},
                                'rickshaw_man': {'type': INT, 'cn': '黄包车夫', 'size': 70,
                                                 'func': tl.CDLRICKSHAWMAN},
                                'marubozu': {'type': INT, 'cn': '光头光脚/缺影线', 'size': 70,
                                             'func': tl.CDLMARUBOZU},
                                'three_inside_up_down': {'type': INT, 'cn': '三内部上涨和下跌', 'size': 70,
                                                         'func': tl.CDL3INSIDE},
                                'three_outside_up_down': {'type': INT, 'cn': '三外部上涨和下跌', 'size': 70,
                                                          'func': tl.CDL3OUTSIDE},
                                'three_stars_in_the_south': {'type': INT, 'cn': '南方三星', 'size': 70,
                                                             'func': tl.CDL3STARSINSOUTH},
                                'three_white_soldiers': {'type': INT, 'cn': '三个白兵', 'size': 70,
                                                         'func': tl.CDL3WHITESOLDIERS},
                                'belt_hold': {'type': INT, 'cn': '捉腰带线', 'size': 70,
                                              'func': tl.CDLBELTHOLD},
                                'breakaway': {'type': INT, 'cn': '脱离', 'size': 70, 'func': tl.CDLBREAKAWAY},
                                'concealing_baby_swallow': {'type': INT, 'cn': '藏婴吞没', 'size': 70,
                                                            'func': tl.CDLCONCEALBABYSWALL},
                                'counterattack': {'type': INT, 'cn': '反击线', 'size': 70,
                                                  'func': tl.CDLCOUNTERATTACK},
                                'dragonfly_doji': {'type': INT, 'cn': '蜻蜓十字/T形十字', 'size': 70,
                                                   'func': tl.CDLDRAGONFLYDOJI},
                                'evening_star': {'type': INT, 'cn': '暮星', 'size': 70,
                                                 'func': tl.CDLEVENINGSTAR},
                                'gravestone_doji': {'type': INT, 'cn': '墓碑十字/倒T十字', 'size': 70,
                                                    'func': tl.CDLGRAVESTONEDOJI},
                                'hammer': {'type': INT, 'cn': '锤头', 'size': 70, 'func': tl.CDLHAMMER},
                                'harami_pattern': {'type': INT, 'cn': '母子线', 'size': 70,
                                                   'func': tl.CDLHARAMI},
                                'harami_cross_pattern': {'type': INT, 'cn': '十字孕线', 'size': 70,
                                                         'func': tl.CDLHARAMICROSS},
                                'homing_pigeon': {'type': INT, 'cn': '家鸽', 'size': 70,
                                                  'func': tl.CDLHOMINGPIGEON},
                                'inverted_hammer': {'type': INT, 'cn': '倒锤头', 'size': 70,
                                                    'func': tl.CDLINVERTEDHAMMER},
                                'kicking': {'type': INT, 'cn': '反冲形态', 'size': 70, 'func': tl.CDLKICKING},
                                'kicking_bull_bear': {'type': INT, 'cn': '由较长缺影线决定的反冲形态',
                                                      'size': 70,
                                                      'func': tl.CDLKICKINGBYLENGTH},
                                'ladder_bottom': {'type': INT, 'cn': '梯底', 'size': 70,
                                                  'func': tl.CDLLADDERBOTTOM},
                                'long_line_candle': {'type': INT, 'cn': '长蜡烛', 'size': 70,
                                                     'func': tl.CDLLONGLINE},
                                'matching_low': {'type': INT, 'cn': '相同低价', 'size': 70,
                                                 'func': tl.CDLMATCHINGLOW},
                                'mat_hold': {'type': INT, 'cn': '铺垫', 'size': 70, 'func': tl.CDLMATHOLD},
                                'morning_doji_star': {'type': INT, 'cn': '十字晨星', 'size': 70,
                                                      'func': tl.CDLMORNINGDOJISTAR},
                                'morning_star': {'type': INT, 'cn': '晨星', 'size': 70,
                                                 'func': tl.CDLMORNINGSTAR},
                                'piercing_pattern': {'type': INT, 'cn': '刺透形态', 'size': 70,
                                                     'func': tl.CDLPIERCING},
                                'rising_falling_three': {'type': INT, 'cn': '上升/下降三法', 'size': 70,
                                                         'func': tl.CDLRISEFALL3METHODS},
                                'separating_lines': {'type': INT, 'cn': '分离线', 'size': 70,
                                                     'func': tl.CDLSEPARATINGLINES},
                                'short_line_candle': {'type': INT, 'cn': '短蜡烛', 'size': 70,
                                                      'func': tl.CDLSHORTLINE},
                                'spinning_top': {'type': INT, 'cn': '纺锤', 'size': 70,
                                                 'func': tl.CDLSPINNINGTOP},
                                'stick_sandwich': {'type': INT, 'cn': '条形三明治', 'size': 70,
                                                   'func': tl.CDLSTICKSANDWICH},
                                'takuri': {'type': INT, 'cn': '探水竿', 'size': 70, 'func': tl.CDLTAKURI},
                                'tasuki_gap': {'type': INT, 'cn': '跳空并列阴阳线', 'size': 70,
                                               'func': tl.CDLTASUKIGAP},
                                'tristar_pattern': {'type': INT, 'cn': '三星', 'size': 70,
                                                    'func': tl.CDLTRISTAR},
                                'unique_3_river': {'type': INT, 'cn': '奇特三河床', 'size': 70,
                                                   'func': tl.CDLUNIQUE3RIVER},
                                'upside_downside_gap': {'type': INT, 'cn': '上升/下降跳空三法', 'size': 70,
                                                        'func': tl.CDLXSIDEGAP3METHODS}}}

TABLE_CN_STOCK_KLINE_PATTERN = {'name': 'cn_stock_pattern', 'cn': '股票K线形态',
                                'columns': TABLE_CN_STOCK_FOREIGN_KEY['columns'].copy()}
TABLE_CN_STOCK_KLINE_PATTERN['columns'].update(STOCK_KLINE_PATTERN_DATA['columns'])

TABLE_CN_STOCK_SELECTION = {'name': 'cn_stock_selection', 'cn': '综合选股',
                            'columns': {'date': {'type': DATE, 'cn': '日期', 'size': 0, 'map': 'MAX_TRADE_DATE', 'en': 'date'},
                                        'code': {'type': INT, 'cn': '代码', 'size': 10, 'map': 'SECURITY_CODE', 'en': 'code'},
                                        'name': {'type': VARCHAR(20, _COLLATE), 'cn': '名称', 'size': 70,'map': 'SECURITY_NAME_ABBR', 'en': 'name'},
                                        'secucode': {'type': VARCHAR(20, _COLLATE), 'cn': '证券代码', 'size': 70,'map': 'SECUCODE', 'en': 'secucode'},
                                        'new_price': {'type': FLOAT, 'cn': '最新价', 'size': 70, 'map': 'NEW_PRICE', 'en': 'new_price'},
                                        'change_rate': {'type': FLOAT, 'cn': '涨跌幅', 'size': 70,
                                                        'map': 'CHANGE_RATE', 'en': 'change_rate'},
                                        'volume_ratio': {'type': FLOAT, 'cn': '量比', 'size': 70,
                                                         'map': 'VOLUME_RATIO', 'en': 'volume_ratio'},
                                        'high_price': {'type': FLOAT, 'cn': '最高价', 'size': 70, 'map': 'HIGH_PRICE', 'en': 'high_price'},
                                        'low_price': {'type': FLOAT, 'cn': '最低价', 'size': 70, 'map': 'LOW_PRICE', 'en': 'low_price'},
                                        'pre_close_price': {'type': FLOAT, 'cn': '昨收价', 'size': 70,
                                                            'map': 'PRE_CLOSE_PRICE', 'en': 'pre_close_price'},
                                        'volume': {'type': BIGINT, 'cn': '成交量', 'size': 90, 'map': 'VOLUME', 'en': 'volume'},
                                        'deal_amount': {'type': BIGINT, 'cn': '成交额', 'size': 100,
                                                        'map': 'DEAL_AMOUNT', 'en': 'deal_amount'},
                                        'turnoverrate': {'type': FLOAT, 'cn': '换手率', 'size': 70,
                                                         'map': 'TURNOVERRATE', 'en': 'turnoverrate'},
                                        'listing_date': {'type': DATE, 'cn': '上市时间', 'size': 110,
                                                         'map': 'LISTING_DATE', 'en': 'listing_date'},
                                        'industry': {'type': VARCHAR(50, _COLLATE), 'cn': '行业', 'size': 100,
                                                     'map': 'INDUSTRY', 'en': 'industry'},
                                        'area': {'type': VARCHAR(50, _COLLATE), 'cn': '地区', 'size': 70, 'map': 'AREA', 'en': 'area'},
                                        'concept': {'type': VARCHAR(800, _COLLATE), 'cn': '概念', 'size': 150,
                                                    'map': 'CONCEPT', 'en': 'concept'},
                                        'style': {'type': VARCHAR(255, _COLLATE), 'cn': '板块', 'size': 150,
                                                  'map': 'STYLE', 'en': 'style'},
                                        'is_hs300': {'type': VARCHAR(2, _COLLATE), 'cn': '沪300', 'size': 0,
                                                     'map': 'IS_HS300', 'en': 'is_hs300'},
                                        'is_sz50': {'type': VARCHAR(2, _COLLATE), 'cn': '上证50', 'size': 0,
                                                     'map': 'IS_SZ50', 'en': 'is_sz50'},
                                        'is_zz500': {'type': VARCHAR(2, _COLLATE), 'cn': '中证500', 'size': 0,
                                                     'map': 'IS_ZZ500', 'en': 'is_zz500'},
                                        'is_zz1000': {'type': VARCHAR(2, _COLLATE), 'cn': '中证1000', 'size': 0,
                                                      'map': 'IS_ZZ1000', 'en': 'is_zz1000'},
                                        'is_cy50': {'type': VARCHAR(2, _COLLATE), 'cn': '创业板50', 'size': 0,
                                                    'map': 'IS_CY50', 'en': 'is_cy50'},
                                        'pe9': {'type': FLOAT, 'cn': '市盈率TTM', 'size': 70, 'map': 'PE9', 'en': 'pe9'},
                                        'pbnewmrq': {'type': FLOAT, 'cn': '市净率MRQ', 'size': 70, 'map': 'PBNEWMRQ', 'en': 'pbnewmrq'},
                                        'pettmdeducted': {'type': FLOAT, 'cn': '市盈率TTM扣非', 'size': 70,
                                                          'map': 'PETTMDEDUCTED', 'en': 'pettmdeducted'},
                                        'ps9': {'type': FLOAT, 'cn': '市销率TTM', 'size': 70, 'map': 'PS9', 'en': 'ps9'},
                                        'pcfjyxjl9': {'type': FLOAT, 'cn': '市现率TTM', 'size': 70, 'map': 'PCFJYXJL9', 'en': 'pcfjyxjl9'},
                                        'predict_pe_syear': {'type': FLOAT, 'cn': '预测市盈率今年', 'size': 70,
                                                             'map': 'PREDICT_PE_SYEAR', 'en': 'predict_pe_syear'},
                                        'predict_pe_nyear': {'type': FLOAT, 'cn': '预测市盈率明年', 'size': 70,
                                                             'map': 'PREDICT_PE_NYEAR', 'en': 'predict_pe_nyear'},
                                        'total_market_cap': {'type': BIGINT, 'cn': '总市值', 'size': 120,
                                                             'map': 'TOTAL_MARKET_CAP', 'en': 'total_market_cap'},
                                        'free_cap': {'type': BIGINT, 'cn': '流通市值', 'size': 120, 'map': 'FREE_CAP', 'en': 'free_cap'},
                                        'dtsyl': {'type': FLOAT, 'cn': '动态市盈率', 'size': 70, 'map': 'DTSYL', 'en': 'dtsyl'},
                                        'ycpeg': {'type': FLOAT, 'cn': '预测PEG', 'size': 70, 'map': 'YCPEG', 'en': 'ycpeg'},
                                        'enterprise_value_multiple': {'type': FLOAT, 'cn': '企业价值倍数', 'size': 70,
                                                                      'map': 'ENTERPRISE_VALUE_MULTIPLE', 'en': 'enterprise_value_multiple'},
                                        'basic_eps': {'type': FLOAT, 'cn': '每股收益', 'size': 70, 'map': 'BASIC_EPS', 'en': 'basic_eps'},
                                        'bvps': {'type': FLOAT, 'cn': '每股净资产', 'size': 70, 'map': 'BVPS', 'en': 'bvps'},
                                        'per_netcash_operate': {'type': FLOAT, 'cn': '每股经营现金流', 'size': 70,
                                                                'map': 'PER_NETCASH_OPERATE', 'en': 'per_netcash_operate'},
                                        'per_fcfe': {'type': FLOAT, 'cn': '每股自由现金流', 'size': 70,
                                                     'map': 'PER_FCFE', 'en': 'per_fcfe'},
                                        'per_capital_reserve': {'type': FLOAT, 'cn': '每股资本公积', 'size': 70,
                                                                'map': 'PER_CAPITAL_RESERVE', 'en': 'per_capital_reserve'},
                                        'per_unassign_profit': {'type': FLOAT, 'cn': '每股未分配利润', 'size': 70,
                                                                'map': 'PER_UNASSIGN_PROFIT', 'en': 'per_unassign_profit'},
                                        'per_surplus_reserve': {'type': FLOAT, 'cn': '每股盈余公积', 'size': 70,
                                                                'map': 'PER_SURPLUS_RESERVE', 'en': 'per_surplus_reserve'},
                                        'per_retained_earning': {'type': FLOAT, 'cn': '每股留存收益', 'size': 70,
                                                                 'map': 'PER_RETAINED_EARNING', 'en': 'per_retained_earning'},
                                        'parent_netprofit': {'type': BIGINT, 'cn': '归属净利润', 'size': 110,
                                                             'map': 'PARENT_NETPROFIT', 'en': 'parent_netprofit'},
                                        'deduct_netprofit': {'type': BIGINT, 'cn': '扣非净利润', 'size': 110,
                                                             'map': 'DEDUCT_NETPROFIT', 'en': 'deduct_netprofit'},
                                        'total_operate_income': {'type': BIGINT, 'cn': '营业总收入', 'size': 120,
                                                                 'map': 'TOTAL_OPERATE_INCOME', 'en': 'total_operate_income'},
                                        'roe_weight': {'type': FLOAT, 'cn': '净资产收益率ROE', 'size': 70,
                                                       'map': 'ROE_WEIGHT', 'en': 'roe_weight'},
                                        'jroa': {'type': FLOAT, 'cn': '总资产净利率ROA', 'size': 70, 'map': 'JROA', 'en': 'jroa'},
                                        'roic': {'type': FLOAT, 'cn': '投入资本回报率ROIC', 'size': 70, 'map': 'ROIC', 'en': 'roic'},
                                        'zxgxl': {'type': FLOAT, 'cn': '最新股息率', 'size': 70, 'map': 'ZXGXL', 'en': 'zxgxl'},
                                        'sale_gpr': {'type': FLOAT, 'cn': '毛利率', 'size': 70, 'map': 'SALE_GPR', 'en': 'sale_gpr'},
                                        'sale_npr': {'type': FLOAT, 'cn': '净利率', 'size': 70, 'map': 'SALE_NPR', 'en': 'sale_npr'},
                                        'netprofit_yoy_ratio': {'type': FLOAT, 'cn': '净利润增长率', 'size': 70,
                                                                'map': 'NETPROFIT_YOY_RATIO', 'en': 'netprofit_yoy_ratio'},
                                        'deduct_netprofit_growthrate': {'type': FLOAT, 'cn': '扣非净利润增长率',
                                                                        'size': 70,
                                                                        'map': 'DEDUCT_NETPROFIT_GROWTHRATE', 'en': 'deduct_netprofit_growthrate'},
                                        'toi_yoy_ratio': {'type': FLOAT, 'cn': '营收增长率', 'size': 70,
                                                          'map': 'TOI_YOY_RATIO', 'en': 'toi_yoy_ratio'},
                                        'netprofit_growthrate_3y': {'type': FLOAT, 'cn': '净利润3年复合增长率',
                                                                    'size': 70,
                                                                    'map': 'NETPROFIT_GROWTHRATE_3Y', 'en': 'netprofit_growthrate_3y'},
                                        'income_growthrate_3y': {'type': FLOAT, 'cn': '营收3年复合增长率', 'size': 70,
                                                                 'map': 'INCOME_GROWTHRATE_3Y', 'en': 'income_growthrate_3y'},
                                        'predict_netprofit_ratio': {'type': FLOAT, 'cn': '预测净利润同比增长',
                                                                    'size': 70,
                                                                    'map': 'PREDICT_NETPROFIT_RATIO', 'en': 'predict_netprofit_ratio'},
                                        'predict_income_ratio': {'type': FLOAT, 'cn': '预测营收同比增长', 'size': 70,
                                                                 'map': 'PREDICT_INCOME_RATIO', 'en': 'predict_income_ratio'},
                                        'basiceps_yoy_ratio': {'type': FLOAT, 'cn': '每股收益同比增长率', 'size': 70,
                                                               'map': 'BASICEPS_YOY_RATIO', 'en': 'basiceps_yoy_ratio'},
                                        'total_profit_growthrate': {'type': FLOAT, 'cn': '利润总额同比增长率',
                                                                    'size': 70,
                                                                    'map': 'TOTAL_PROFIT_GROWTHRATE', 'en': 'total_profit_growthrate'},
                                        'operate_profit_growthrate': {'type': FLOAT, 'cn': '营业利润同比增长率',
                                                                      'size': 70,
                                                                      'map': 'OPERATE_PROFIT_GROWTHRATE', 'en': 'operate_profit_growthrate'},
                                        'debt_asset_ratio': {'type': FLOAT, 'cn': '资产负债率', 'size': 70,
                                                             'map': 'DEBT_ASSET_RATIO', 'en': 'debt_asset_ratio'},
                                        'equity_ratio': {'type': FLOAT, 'cn': '产权比率', 'size': 70,
                                                         'map': 'EQUITY_RATIO', 'en': 'equity_ratio'},
                                        'equity_multiplier': {'type': FLOAT, 'cn': '权益乘数', 'size': 70,
                                                              'map': 'EQUITY_MULTIPLIER', 'en': 'equity_multiplier'},
                                        'current_ratio': {'type': FLOAT, 'cn': '流动比率', 'size': 70,
                                                          'map': 'CURRENT_RATIO', 'en': 'current_ratio'},
                                        'speed_ratio': {'type': FLOAT, 'cn': '速动比率', 'size': 70,
                                                         'map': 'SPEED_RATIO', 'en': 'speed_ratio'},
                                        'total_shares': {'type': BIGINT, 'cn': '总股本', 'size': 120,
                                                         'map': 'TOTAL_SHARES', 'en': 'total_shares'},
                                        'free_shares': {'type': BIGINT, 'cn': '流通股本', 'size': 120,
                                                         'map': 'FREE_SHARES', 'en': 'free_shares'},
                                        'holder_newest': {'type': BIGINT, 'cn': '最新股东户数', 'size': 80,
                                                          'map': 'HOLDER_NEWEST', 'en': 'holder_newest'},
                                        'holder_ratio': {'type': FLOAT, 'cn': '股东户数增长率', 'size': 70,
                                                         'map': 'HOLDER_RATIO', 'en': 'holder_ratio'},
                                        'hold_amount': {'type': FLOAT, 'cn': '户均持股金额', 'size': 80,
                                                         'map': 'HOLD_AMOUNT', 'en': 'hold_amount'},
                                        'avg_hold_num': {'type': FLOAT, 'cn': '户均持股数量', 'size': 70,
                                                         'map': 'AVG_HOLD_NUM', 'en': 'avg_hold_num'},
                                        'holdnum_growthrate_3q': {'type': FLOAT, 'cn': '户均持股数季度增长率',
                                                                  'size': 70,
                                                                  'map': 'HOLDNUM_GROWTHRATE_3Q', 'en': 'holdnum_growthrate_3q'},
                                        'holdnum_growthrate_hy': {'type': FLOAT, 'cn': '户均持股数半年增长率',
                                                                  'size': 70,
                                                                  'map': 'HOLDNUM_GROWTHRATE_HY', 'en': 'holdnum_growthrate_hy'},
                                        'hold_ratio_count': {'type': FLOAT, 'cn': '十大股东持股比例合计', 'size': 70,
                                                             'map': 'HOLD_RATIO_COUNT', 'en': 'hold_ratio_count'},
                                        'free_hold_ratio': {'type': FLOAT, 'cn': '十大流通股东比例合计', 'size': 70,
                                                             'map': 'FREE_HOLD_RATIO', 'en': 'free_hold_ratio'},
                                        'macd_golden_fork': {'type': BIT, 'cn': 'MACD金叉日线', 'size': 70,
                                                              'map': 'MACD_GOLDEN_FORK', 'en': 'macd_golden_fork'},
                                        'macd_golden_forkz': {'type': BIT, 'cn': 'MACD金叉周线', 'size': 70,
                                                              'map': 'MACD_GOLDEN_FORKZ', 'en': 'macd_golden_forkz'},
                                        'macd_golden_forky': {'type': BIT, 'cn': 'MACD金叉月线', 'size': 70,
                                                              'map': 'MACD_GOLDEN_FORKY', 'en': 'macd_golden_forky'},
                                        'kdj_golden_fork': {'type': BIT, 'cn': 'KDJ金叉日线', 'size': 70,
                                                            'map': 'KDJ_GOLDEN_FORK', 'en': 'kdj_golden_fork'},
                                        'kdj_golden_forkz': {'type': BIT, 'cn': 'KDJ金叉周线', 'size': 70,
                                                              'map': 'KDJ_GOLDEN_FORKZ', 'en': 'kdj_golden_forkz'},
                                        'kdj_golden_forky': {'type': BIT, 'cn': 'KDJ金叉月线', 'size': 70,
                                                              'map': 'KDJ_GOLDEN_FORKY', 'en': 'kdj_golden_forky'},
                                        'break_through': {'type': BIT, 'cn': '放量突破', 'size': 70,
                                                          'map': 'BREAK_THROUGH', 'en': 'break_through'},
                                        'low_funds_inflow': {'type': BIT, 'cn': '低位资金净流入', 'size': 70,
                                                             'map': 'LOW_FUNDS_INFLOW', 'en': 'low_funds_inflow'},
                                        'high_funds_outflow': {'type': BIT, 'cn': '高位资金净流出', 'size': 70,
                                                               'map': 'HIGH_FUNDS_OUTFLOW', 'en': 'high_funds_outflow'},
                                        'breakup_ma_5days': {'type': BIT, 'cn': '向上突破均线5日', 'size': 70,
                                                             'map': 'BREAKUP_MA_5DAYS', 'en': 'breakup_ma_5days'},
                                        'breakup_ma_10days': {'type': BIT, 'cn': '向上突破均线10日', 'size': 70,
                                                              'map': 'BREAKUP_MA_10DAYS', 'en': 'breakup_ma_10days'},
                                        'breakup_ma_20days': {'type': BIT, 'cn': '向上突破均线20日', 'size': 70,
                                                              'map': 'BREAKUP_MA_20DAYS', 'en': 'breakup_ma_20days'},
                                        'breakup_ma_30days': {'type': BIT, 'cn': '向上突破均线30日', 'size': 70,
                                                              'map': 'BREAKUP_MA_30DAYS', 'en': 'breakup_ma_30days'},
                                        'breakup_ma_60days': {'type': BIT, 'cn': '向上突破均线60日', 'size': 70,
                                                              'map': 'BREAKUP_MA_60DAYS', 'en': 'breakup_ma_60days'},
                                        'long_avg_array': {'type': BIT, 'cn': '均线多头排列', 'size': 70,
                                                           'map': 'LONG_AVG_ARRAY', 'en': 'long_avg_array'},
                                        'short_avg_array': {'type': BIT, 'cn': '均线空头排列', 'size': 70,
                                                            'map': 'SHORT_AVG_ARRAY', 'en': 'short_avg_array'},
                                        'upper_large_volume': {'type': BIT, 'cn': '高位大成交量', 'size': 70,
                                                               'map': 'UPPER_LARGE_VOLUME', 'en': 'upper_large_volume'}
                                        }
                            }    

CN_STOCK_CPBD = {'name': 'cn_stock_cpbd', 'cn': '操盘必读',
                 'columns': {'SECURITY_CODE': {'type': VARCHAR(6, _COLLATE), 'cn': '代码'},
                             'SECURITY_NAME_ABBR': {'type': VARCHAR(20, _COLLATE), 'cn': '名称'},
                             'PE_DYNAMIC': {'type': FLOAT, 'cn': '市盈率动'},
                             'PE_TTM': {'type': FLOAT, 'cn': '市盈率TTM'},
                             'PE_STATIC': {'type': FLOAT, 'cn': '市盈率静'},
                             'PB_MRQ_REALTIME': {'type': FLOAT, 'cn': '市净率'},
                             'REPORT_DATE': {'type': DATE, 'cn': '财报期'},
                             'EPSJB': {'type': FLOAT, 'cn': '每股收益'},
                             'BPS': {'type': FLOAT, 'cn': '每股净资产'},
                             'MGJYXJJE': {'type': FLOAT, 'cn': '每股经营现金流'},
                             'MGZBGJ': {'type': FLOAT, 'cn': '每股公积金'},
                             'MGWFPLR': {'type': FLOAT, 'cn': '每股未分配利润'},
                             'ROEJQ': {'type': FLOAT, 'cn': '加权净资产收益率'},
                             'XSMLL': {'type': FLOAT, 'cn': '毛利率'},
                             'ZCFZL': {'type': FLOAT, 'cn': '资产负债率'},
                             'TOTAL_OPERATEINCOME': {'type': FLOAT, 'cn': '营业收入'},
                             'YYZSRGDHBZC': {'type': FLOAT, 'cn': '营业收入滚动环比增长'},
                             'TOTALOPERATEREVETZ': {'type': FLOAT, 'cn': '营业收入同比增长'},
                             'PARENT_NETPROFIT': {'type': FLOAT, 'cn': '归属净利润'},
                             'NETPROFITRPHBZC': {'type': FLOAT, 'cn': '归属净利润滚动环比增长'},
                             'PARENTNETPROFITTZ': {'type': FLOAT, 'cn': '归属净利润同比增长'},
                             'KCFJCXSYJLR': {'type': FLOAT, 'cn': '扣非净利润'},
                             'KFJLRGDHBZC': {'type': FLOAT, 'cn': '扣非净利润滚动环比增长'},
                             'KCFJCXSYJLRTZ': {'type': FLOAT, 'cn': '扣非净利润同比增长'},
                             'TOTAL_SHARE': {'type': FLOAT, 'cn': '总股本'},
                             'FREE_SHARE': {'type': FLOAT, 'cn': '流通股本'},
                             'BOARD_NAME': {'type': VARCHAR(20, _COLLATE), 'cn': '所属板块'},
                             'END_DATE': {'type': DATE, 'cn': '股东日'},
                             'HOLDER_TOTAL_NUM': {'type': FLOAT, 'cn': '股东人数'},
                             'TOTAL_NUM_RATIO': {'type': FLOAT, 'cn': '较上期变化'},
                             'AVG_FREE_SHARES': {'type': FLOAT, 'cn': '人均流通股'},
                             'AVG_FREESHARES_RATIO': {'type': FLOAT, 'cn': '较上期变化'},
                             'HOLD_FOCUS': {'type': FLOAT, 'cn': '筹码集中度'},
                             'AVG_HOLD_AMT': {'type': FLOAT, 'cn': '人均持股金额'},
                             'HOLD_RATIO_TOTAL': {'type': FLOAT, 'cn': '十大股东持股'},
                             'FREEHOLD_RATIO_TOTAL': {'type': FLOAT, 'cn': '十大流通股东持股'},
                             'LHBD_DATE': {'type': DATE, 'cn': '龙虎榜日'},
                             'EXPLANATION': {'type': FLOAT, 'cn': '龙虎说明'},
                             'TOTAL_BUY': {'type': FLOAT, 'cn': '买入金额'},
                             'TOTAL_BUYRIOTOP': {'type': FLOAT, 'cn': '买入占比'},
                             'TOTAL_SELL': {'type': FLOAT, 'cn': '卖出金额'},
                             'TOTAL_SELLRIOTOP': {'type': FLOAT, 'cn': '卖出占比'},
                             'DZJY_DATE': {'type': DATE, 'cn': '大宗交易日'},
                             'DEAL_PRICE': {'type': FLOAT, 'cn': '成交价'},
                             'PREMIUM_RATIO': {'type': FLOAT, 'cn': '折溢价率'},
                             'DEAL_VOLUME': {'type': FLOAT, 'cn': '成交量'},
                             'DEAL_AMT': {'type': FLOAT, 'cn': '成交额'},
                             'BUYER_NAME': {'type': FLOAT, 'cn': '买营业部'},
                             'SELLER_NAME': {'type': FLOAT, 'cn': '卖营业部'},
                             'RZRQ_DATE': {'type': DATE, 'cn': '融资券日'},
                             'FIN_BUY_AMT': {'type': FLOAT, 'cn': '融资买额'},
                             'FIN_REPAY_AMT': {'type': FLOAT, 'cn': '融资还额'},
                             'FIN_BALANCE': {'type': FLOAT, 'cn': '融资余额'},
                             'LOAN_SELL_VOL': {'type': FLOAT, 'cn': '融券卖量'},
                             'LOAN_REPAY_VOL': {'type': FLOAT, 'cn': '融券还量'},
                             'LOAN_BALANCE': {'type': FLOAT, 'cn': '融券余额'}}}




# 表注册中心（集中管理所有表结构）
TABLE_REGISTRY = {
    # ----------------------
    # 基础信息表
    # ----------------------
    'cn_stock_info': TABLE_STOCK_INIT,
    'cn_etf_info': TABLE_ETF_INIT,
    'cn_index_info': TABLE_INDEX_INIT,

    # ----------------------
    # 实时行情表
    # ----------------------
    'cn_stock_selection': TABLE_CN_STOCK_SELECTION,
    'cn_stock_spot': TABLE_CN_STOCK_SPOT,
    'cn_etf_spot': TABLE_CN_ETF_SPOT,
    'cn_index_spot': TABLE_CN_INDEX_SPOT,

    # ----------------------
    # 资金流向表
    # ----------------------
    'cn_stock_fund_flow': TABLE_CN_STOCK_FUND_FLOW,
    'cn_stock_fund_flow_industry': TABLE_CN_STOCK_FUND_FLOW_INDUSTRY,
    'cn_stock_fund_flow_concept': TABLE_CN_STOCK_FUND_FLOW_CONCEPT,

    # ----------------------
    # 历史数据表
    # ----------------------
    'cn_stock_hist': CN_STOCK_HIST_DATA,

    # 股票历史（新增周期维度）
    # 股票历史
    'cn_stock_hist_daily': CN_STOCK_HIST_DAILY_DATA,
    'cn_stock_hist_weekly': CN_STOCK_HIST_WEEKLY_DATA,
    'cn_stock_hist_monthly': CN_STOCK_HIST_MONTHLY_DATA,
    
    # ETF历史
    'cn_etf_hist_daily': CN_ETF_HIST_DAILY_DATA,
    'cn_etf_hist_weekly': CN_ETF_HIST_WEEKLY_DATA,
    'cn_etf_hist_monthly': CN_ETF_HIST_MONTHLY_DATA,
    
    # 指数历史
    'cn_index_hist_daily': CN_INDEX_HIST_DAILY_DATA,
    'cn_index_hist_weekly': CN_INDEX_HIST_WEEKLY_DATA,
    'cn_index_hist_monthly': CN_INDEX_HIST_MONTHLY_DATA,


    # ----------------------
    # 选股策略表
    # ----------------------
    'cn_stock_strategy_enter': TABLE_CN_STOCK_STRATEGIES[0],
    'cn_stock_strategy_keep_increasing': TABLE_CN_STOCK_STRATEGIES[1],
    'cn_stock_strategy_parking_apron': TABLE_CN_STOCK_STRATEGIES[2],
    'cn_stock_strategy_backtrace_ma250': TABLE_CN_STOCK_STRATEGIES[3],
    'cn_stock_strategy_breakthrough_platform': TABLE_CN_STOCK_STRATEGIES[4],
    'cn_stock_strategy_low_backtrace_increase': TABLE_CN_STOCK_STRATEGIES[5],
    'cn_stock_strategy_turtle_trade': TABLE_CN_STOCK_STRATEGIES[6],
    'cn_stock_strategy_high_tight_flag': TABLE_CN_STOCK_STRATEGIES[7],
    'cn_stock_strategy_climax_limitdown': TABLE_CN_STOCK_STRATEGIES[8],
    'cn_stock_strategy_low_atr': TABLE_CN_STOCK_STRATEGIES[9],

    # ----------------------
    # 指标与回测表
    # ----------------------
    'cn_stock_indicators': TABLE_CN_STOCK_INDICATORS,
    'cn_stock_indicators_buy': TABLE_CN_STOCK_INDICATORS_BUY,
    'cn_stock_indicators_sell': TABLE_CN_STOCK_INDICATORS_SELL,
    'cn_stock_backtest_data': TABLE_CN_STOCK_BACKTEST_DATA,

    'cn_etf_indicators': TABLE_CN_ETF_INDICATORS,
    'cn_etf_indicators_buy': TABLE_CN_ETF_INDICATORS_BUY,
    'cn_etf_indicators_sell': TABLE_CN_ETF_INDICATORS_SELL,
    'cn_etf_backtest_data': TABLE_CN_ETF_BACKTEST_DATA,

    'cn_index_indicators': TABLE_CN_INDEX_INDICATORS,
    'cn_index_indicators_buy': TABLE_CN_INDEX_INDICATORS_BUY,
    'cn_index_indicators_sell': TABLE_CN_INDEX_INDICATORS_SELL,
    'cn_index_backtest_data': TABLE_CN_INDEX_BACKTEST_DATA,

    # ----------------------
    # 其他功能表
    # ----------------------
    'cn_stock_attention': TABLE_CN_STOCK_ATTENTION,
    'cn_stock_bonus': TABLE_CN_STOCK_BONUS,
    'cn_stock_blocktrade': TABLE_CN_STOCK_BLOCKTRADE,
    'cn_stock_pattern': TABLE_CN_STOCK_KLINE_PATTERN,
    'cn_stock_cpbd': CN_STOCK_CPBD
}


def get_field_cn(key, table):
    f = table.get('columns').get(key)
    if f is None:
        return key
    return f.get('cn')


def get_field_cns(cols):
    data = []
    for k in cols:
        if k == 'code':
            data.append({"value": k, "caption": cols[k]['cn'], "width": cols[k]['size'],
                         "headerStyle": {"font": "bold 9pt Calibri", "wordWrap": "true"}, "style": ""})
        elif k == 'change_rate':
            data.append({"value": k, "caption": cols[k]['cn'], "width": cols[k]['size'],
                         "headerStyle": {"font": "bold 9pt Calibri", "wordWrap": "true"}, "conditionalFormats": [
                    {"ruleType": "formulaRule", "formula": "@>0", "style": {"foreColor": "red"}},
                    {"ruleType": "formulaRule", "formula": "@<0", "style": {"foreColor": "green"}}]})
        else:
            data.append({"value": k, "caption": cols[k]['cn'], "width": cols[k]['size'],
                         "headerStyle": {"font": "bold 9pt Calibri", "wordWrap": "true"}})
        # data.append({"value": k, "caption": cols[k]['cn'], "width": cols[k]['size'], "headerStyle": {"font": "bold 9pt Calibri", "wordWrap": "true"}})
        # data.append({"name": k, "displayName": cols[k]['cn'], "size": cols[k]['size']})
    return data


def get_field_types(cols):
    data = {}
    for k in cols:
        data[k] = cols[k]['type']
    return data


def get_field_type_name(col_type):
    if col_type == DATE:
        return "datetime"
    elif col_type == FLOAT or col_type == BIGINT or col_type == INT or col_type == BIT:
        return "numeric"
    else:
        return "string"

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
    elif isinstance(py_type, BIT):
        # return "BIT"
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
    elif py_type == BIT:
        return "BOOLEAN"  # 直接映射为 MySQL 的 BOOLEAN 类型
        # return "BIT"
    
    raise ValueError(f"Unsupported type: {py_type}")

