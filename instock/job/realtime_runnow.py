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
import init_job as bj
import basic_data_daily_job as hdj
import basic_data_other_daily_job as hdtj
import basic_data_after_close_daily_job as acdj
import indicators_data_daily_job as gdj
import strategy_data_daily_job as sdj
import backtest_data_daily_job as backtest_data_daily_job
import klinepattern_data_daily_job as kdj
import selection_data_daily_job as sddj
import basic_data as basic_data
import basic_hist_data as basic_hist_data
import indicators_data_daily as indicators_data_daily
import threeday_indicators as threeday_indicators
import indicators_strategy_buy as indicators_strategy_buy
import market_sentiment_a as market_sentiment_a
import industry_data as industry_data
import industry_sentiment_a as industry_sentiment_a

import strategy_etf_buy as strategy_etf_buy
import strategy_stock_buy_optimization as strategy_stock_buy_optimization
import strategy_stock_sell as strategy_stock_sell

import create_realtime_table as create_realtime_table
import create_kline_table as create_kline_table
import create_info_table as create_info_table

import realtime_stock as realtime_stock
import realtime_index as realtime_index
# import realtime_etf as realtime_etf


import kline_stock as kline_stock
import kline_index as kline_index
# import kline_etf as kline_etf

__author__ = 'myh '
__date__ = '2023/3/10 '


def main():
    start = time.time()
    _start = datetime.datetime.now()
    logging.info("######## 任务执行时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))

    # 第一步
    # 创建实时数据表
    create_realtime_table.create_tables()
    # 创建K线数据表
    create_kline_table.create_tables()
    # 创建基础信息数据表
    create_info_table.create_tables()

    # 第二步
    # 获取实时股票数据
    realtime_stock.main()
    # 获取实时指数数据
    realtime_index.main()
    # 获取实时基金数据
    # realtime_etf.main()

    # 第三步
    # 写入K线股票数据
    kline_stock.main()
    # 获写入K线指数数据
    kline_index.main()
    # 写入K线基金数据
    # kline_etf.main()


    indicators_data_daily.main()

    threeday_indicators.main()

    market_sentiment_a.main()

    industry_data.main()

    industry_sentiment_a.main()

    indicators_strategy_buy.main()

    buy_20250425.main()

    # 获取实时股票、基金、指数数据并同时创建和写入数据主表，数据基础表
    # basic_data.main()

    # 通过数据基础表，获取所有股票、基金、指数的历史日、周、月行情数据，并同时创建和写入历史数据主表
    # basic_hist_data.main()

    # 通过数据基础表，获取历史数据表中个股数据，计算所有股票、基金、指数的日行情指标，并同时创建和写入日行情指标主表
    # indicators_data_daily.main()

    # 指标数据处理
    # threeday_indicators.main()

    # 指标情绪
    # market_sentiment_a.main()

    # 行业数据处理
    # industry_data.main()

    # 行业情绪
    # industry_sentiment_a.main()

    # 买入策略
    # indicators_strategy_buy.main()
    # 买入策略精选
    # strategy_stock_buy_optimization.main()

    # 卖出策略
    # strategy_stock_sell.main()

    # etf 买入策略
    # strategy_etf_buy.main()

    # 第2.1步创建股票基础数据表
    # hdj.main()
    # 第2.2步创建综合股票数据表
    # sddj.main()
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    # # 第3.1步创建股票其它基础数据表
    # executor.submit(hdtj.main)
    # # 第3.2步创建股票指标数据表
    # executor.submit(gdj.main)
    # # 第3.3步创建ETF指标数据表
    # executor.submit(ieddj.main)
    # # 第3.4步创建指数指标数据表
    # executor.submit(iiddj.main)
    # # # # 第4步创建股票k线形态表
    # executor.submit(kdj.main)
    # # # # 第5步创建股票策略数据表
    # executor.submit(sdj.main)

    # # # # 第6步创建股票回测
    # backtest_data_daily_job.main()

    # # # # 第7步创建股票闭盘后才有的数据
    # acdj.main()

    logging.info("######## 完成任务, 使用时间: %s 秒 #######" % (time.time() - start))


# main函数入口
if __name__ == '__main__':
    main()
