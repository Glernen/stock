#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
from datetime import datetime, timedelta
from threading import Lock

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.indicator.calculate_indicator as idr
from instock.core.singleton_stock import stock_hist_data
# from instock.core.singleton_etf import etf_hist_data
from instock.core.singleton_etf import etf_data, etf_hist_data

__author__ = 'hqm'
__date__ = '2025/3/23'

# 添加线程锁确保单例初始化安全
singleton_lock = Lock()

def prepare(date):
    try:
        # 强制删除单例实例，确保每次处理新日期时重新初始化
        if hasattr(etf_hist_data, "_instance"):
            del etf_hist_data._instance
        if hasattr(etf_data, "_instance"):
            del etf_data._instance

        with singleton_lock:
            logging.info(f"ieddj.py基金行情数据，传入的日期参数: {date}")
            print(f"ieddj.py基金行情数据，传入的日期参数: {date}")
            
            # 强制重新获取数据（确保 force_reload=True）
            etfs_data = etf_hist_data(date=date, force_reload=True).get_data()
            if etfs_data is None:
                logging.error(f"无法获取 {date} 的基金数据")
                return
            
            results = run_check(etfs_data, date=date)
            if results is None:
                logging.error(f"未找到 {date} 的基金指标数据")
                return

            table_name = tbs.TABLE_CN_ETF_INDICATORS['name']
            dataKey = pd.DataFrame(results.keys())
            _columns = tuple(tbs.TABLE_CN_ETF_FOREIGN_KEY['columns'])
            dataKey.columns = _columns

            dataVal = pd.DataFrame(results.values())
            dataVal.drop('date', axis=1, inplace=True)  # 删除日期字段，然后和原始数据合并。

            data = pd.merge(dataKey, dataVal, on=['code'], how='left')
            date_str = date.strftime("%Y-%m-%d")
            if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
                data['date'] = date_str

            # 处理NaN值
            data = data.where(pd.notnull(data), None)

            # 先删除当天的旧数据
            with mdb.engine().begin() as conn:
                delete_sql = text(f"DELETE FROM `{table_name}` WHERE `date` = :date")
                conn.execute(delete_sql, {"date": date_str})

            # 分批插入数据
            chunksize = 1000  # 可以根据实际情况调整
            data.to_sql(
                name=table_name,
                con=mdb.engine(),
                if_exists='append',
                index=False,
                chunksize=chunksize
            )
            logging.info(f"成功插入 {date_str} 的 {len(data)} 条基金指标数据")

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
            # 使用日期参数作为唯一键
            futures = {executor.submit(idr.get_indicator, k, stocks[k], data_column, date=date): (k, date) for k in stocks}
            for future in concurrent.futures.as_completed(futures):
                stock, current_date = futures[future]
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
        _columns_backtest = tuple(tbs.TABLE_CN_ETF_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

        # 先删除当天的旧数据
        date_str = date.strftime("%Y-%m-%d")
        with mdb.engine().begin() as conn:
            delete_sql = text(f"DELETE FROM `{table_name}` WHERE `date` = :date")
            conn.execute(delete_sql, {"date": date_str})

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)
        logging.info(f"成功插入 {date_str} 的 {len(data)} 条基金买入指标数据")

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
        _columns_backtest = tuple(tbs.TABLE_CN_ETF_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

        # 先删除当天的旧数据
        date_str = date.strftime("%Y-%m-%d")
        with mdb.engine().begin() as conn:
            delete_sql = text(f"DELETE FROM `{table_name}` WHERE `date` = :date")
            conn.execute(delete_sql, {"date": date_str})

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)
        logging.info(f"成功插入 {date_str} 的 {len(data)} 条基金卖出指标数据")

    except Exception as e:
        logging.error(f"indicators_etf_data_daily_job.guess_sell处理异常：{e}")



def main():
    # 配置日志
    logging.basicConfig(
        filename='indicators_etf_data_daily_job.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'  # 每次运行清空日志
    )

    if len(sys.argv) == 1:
        # 没有传入日期参数，使用当前日期
        date = datetime.now()
        dates = [date]
    elif len(sys.argv) == 2:
        # 传入单个日期
        date_str = sys.argv[1]
        date = datetime.strptime(date_str, "%Y-%m-%d")
        dates = [date]
    elif len(sys.argv) == 3:
        # 传入日期区间
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
    else:
        print("参数格式错误，请使用以下格式：")
        print("单个日期：python indicators_etf_data_daily_job.py 2024-04-02")
        print("日期区间：python indicators_etf_data_daily_job.py 2024-04-02 2024-04-05")
        return

    for date in dates:
        prepare(date)
        guess_buy(date)
        guess_sell(date)


# main函数入口
if __name__ == '__main__':
    main()