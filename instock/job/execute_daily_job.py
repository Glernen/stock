#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
import datetime
import concurrent.futures
import logging
import os.path
import sys

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(format='%(asctime)s %(message)s', filename=os.path.join(log_path, 'stock_execute_job.log'))
logging.getLogger().setLevel(logging.INFO)

import backtest_data_daily_job as backtest_data_daily_job

import indicators_data_daily as indicators_data_daily
import threeday_indicators as threeday_indicators

import market_sentiment_a as market_sentiment_a
import industry_data as industry_data
import industry_sentiment_a as industry_sentiment_a
import strategy_etf_buy as strategy_etf_buy

import strategy_stock_buy as strategy_stock_buy
import strategy_stock_buy_optimization as strategy_stock_buy_optimization
import strategy_stock_sell as strategy_stock_sell

import create_realtime_table as create_realtime_table
import create_kline_table as create_kline_table
import create_info_table as create_info_table

import realtime_stock as realtime_stock
import realtime_index as realtime_index
import realtime_etf as realtime_etf
import realtime_industry as realtime_industry

import basic_info_stock as basic_info_stock

import kline_stock as kline_stock
import kline_index as kline_index
import kline_etf as kline_etf
import kline_industry as kline_industry

import pattern_stock as pattern_stock
import pattern_etf as pattern_etf
import pattern_index as pattern_index
import pattern_industry as pattern_industry

import histdata as histdata

__author__ = 'myh '
__date__ = '2023/3/10 '


def main():
    start = time.time()
    _start = datetime.datetime.now()
    logging.info("######## 任务执行时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))

    # 第一步
    # 创建实时数据表
    # create_realtime_table.create_tables()
    # 创建K线数据表
    # create_kline_table.create_tables()
    # 创建基础信息数据表
    # create_info_table.create_tables()

    # 第二步
    # 获取实时股票数据
    realtime_stock.main()
    # 获取实时指数数据
    realtime_index.main()
    # 获取实时基金数据
    realtime_etf.main()
    # 获取实时行业数据
    realtime_industry.main()

    # 第三步
    # 写入基础表，完善申一/二行业数据
    basic_info_stock.main()
    # 写入K线股票数据
    kline_stock.main()
    # 获写入K线指数数据
    kline_index.main()
    # 写入K线基金数据
    kline_etf.main()
    # 写入K线行业数据
    kline_industry.main()

    # K线图标
    pattern_stock.main()
    pattern_etf.main()
    pattern_index.main()
    pattern_industry.main()

    # 通过数据基础表，获取历史数据表中个股数据，计算所有股票、基金、指数的日行情指标，并同时创建和写入日行情指标主表
    indicators_data_daily.main()

    # 指标数据处理
    threeday_indicators.main()

    # 指标情绪
    market_sentiment_a.main()

    # 行业数据处理
    industry_data.main()

    # 行业情绪
    industry_sentiment_a.main()
    
    # 买入策略
    strategy_stock_buy.main()
    # 买入策略精选
    strategy_stock_buy_optimization.main()

    # 卖出策略
    strategy_stock_sell.main()

    # etf 买入策略
    strategy_etf_buy.main()

    # # # # 第6步创建股票回测
    backtest_data_daily_job.main()


    histdata.main()

    logging.info("######## 完成任务, 使用时间: %s 秒 #######" % (time.time() - start))


# main函数入口
if __name__ == '__main__':
    main()
