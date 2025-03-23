#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.indicator.calculate_indicator as idr
from instock.core.singleton_stock import stock_hist_data
from instock.core.singleton_etf import etf_hist_data

__author__ = 'hqm'
__date__ = '2025/3/23'



def prepare(date):
    try:
        logging.info(f"ieddj.py基金行情数据，传入的日期参数: {date}")
        print(f"ieddj.py基金行情数据，传入的日期参数: {date}")
        etfs_data = etf_hist_data(date=date).get_data()

        # print(f"ieddj.py基金行情数据，etfs_data：{etfs_data}")
        if etfs_data is None:
            return
        results = run_check(etfs_data, date=date)
        if results is None:
            return

        table_name = tbs.TABLE_CN_ETF_INDICATORS['name']
        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_ETF_FOREIGN_KEY['columns'])
        dataKey.columns = _columns

        dataVal = pd.DataFrame(results.values())
        dataVal.drop('date', axis=1, inplace=True)  # 删除日期字段，然后和原始数据合并。

        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)

    except Exception as e:
        logging.error(f"indicators_etf_data_daily_job.prepare处理异常：{e}")
        print(f"indicators_etf_data_daily_job.prepare处理异常：{e}")


def run_check(stocks, date=None, workers=40):
    data = {}
    columns = list(tbs.STOCK_STATS_DATA['columns'])
    columns.insert(0, 'code')
    columns.insert(0, 'date')
    data_column = columns
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(idr.get_indicator, k, stocks[k], data_column, date=date): k for k in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                except Exception as e:
                    logging.error(f"indicators_etf_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
    except Exception as e:
        logging.error(f"indicators_etf_data_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data


# 对每日指标数据，进行筛选。将符合条件的。二次筛选出来。
# 只是做简单筛选
def guess_buy(date):
    try:
        _table_name = tbs.TABLE_CN_ETF_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = tuple(tbs.TABLE_CN_ETF_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)
        
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and
                `kdjj` <= 0 and `rsi_6` <= 30 and
                `cci` <= -130 and `rsi_12` <= 45 and `close` <= `boll_lb` and
                ABS(`wr_6`) >= 90 and ABS(`wr_10`) >= 90'''

        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_ETF_INDICATORS_BUY['name']
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)

    except Exception as e:
        logging.error(f"indicators_etf_data_daily_job.guess_buy处理异常：{e}")


# 设置卖出数据。
def guess_sell(date):
    try:
        _table_name = tbs.TABLE_CN_ETF_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = tuple(tbs.TABLE_CN_ETF_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)

        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and
                `kdjj` >= 90 and `rsi_6` >= 65 and
                `cci` >= 180 and `rsi_12` >= 65 and `close` >= `boll_ub` and
                ABS(`wr_6`) <= 5'''

        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_ETF_INDICATORS_SELL['name']
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)

    except Exception as e:
        logging.error(f"indicators_etf_data_daily_job.guess_sell处理异常：{e}")


def main():
    # 使用方法传递。
    runt.run_with_args(prepare)
    # 二次筛选数据。直接计算买卖股票数据。
    runt.run_with_args(guess_buy)
    runt.run_with_args(guess_sell)


# main函数入口
if __name__ == '__main__':
    main()
