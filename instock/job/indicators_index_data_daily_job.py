#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import pymysql
import concurrent.futures
import pandas as pd
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import numpy as np
from datetime import datetime, timedelta
import requests
import instock.lib.database as mdb
import instock.core.tablestructure as tbs
import instock.core.indicator.calculate_index_indicator as idr
from functools import lru_cache
import instock.lib.trade_time as trd
from instock.lib.singleton_type import singleton_type
from sqlalchemy import text
from sqlalchemy import Date, Float, String  # 引入 SQLAlchemy 的 Date、Float 和 String 类型
from sqlalchemy import create_engine, MetaData, Table, Column

# 定义 INDEX_STATS_DATA
INDEX_STATS_DATA = {'name': 'calculate_index_indicator', 'cn': '指标计算助手库',
                    'columns': {'close': {'type': Float, 'cn': '价格','size': 0},
                                'macd': {'type': Float, 'cn': 'dif','size': 70},
                                'macds': {'type': Float, 'cn':'macd','size': 70},
                                'macdh': {'type': Float, 'cn': 'histogram','size': 70},
                                'kdjk': {'type': Float, 'cn': 'kdjk','size': 70},
                                'kdjd': {'type': Float, 'cn': 'kdjd','size': 70},
                                'kdjj': {'type': Float, 'cn': 'kdjj','size': 70},
                                'boll_ub': {'type': Float, 'cn': 'boll上轨','size': 70},
                                'boll': {'type': Float, 'cn': 'boll','size': 70},
                                'boll_lb': {'type': Float, 'cn': 'boll下轨','size': 70},
                                'cr-ma1': {'type': Float, 'cn': 'cr-ma1','size': 70},
                                'cr-ma2': {'type': Float, 'cn': 'cr-ma2','size': 70},
                                'cr-ma3': {'type': Float, 'cn': 'cr-ma3','size': 70},
                                'rsi_6': {'type': Float, 'cn': 'rsi_6','size': 70},
                                'rsi_12': {'type': Float, 'cn': 'rsi_12','size': 70},
                                'rsi': {'type': Float, 'cn': 'rsi','size': 70},
                                'rsi_24': {'type': Float, 'cn': 'rsi_24','size': 70},
                                'wr_6': {'type': Float, 'cn': 'wr_6','size': 70},
                                'wr_10': {'type': Float, 'cn': 'wr_10','size': 70},
                                'wr_14': {'type': Float, 'cn': 'wr_14','size': 70},
                                'cci': {'type': Float, 'cn': 'cci','size': 70},
                                'cci_84': {'type': Float, 'cn': 'cci_84','size': 70}}}


def create_table_from_structure(engine, table_name, table_structure):
    metadata = MetaData()
    columns = []
    # 先添加 date 和 code 列
    columns.append(Column('date', Date))
    columns.append(Column('code', String(6)))  # 设置 code 为 varchar(6)
    # 添加 name 列，设置为 varchar(20)
    columns.append(Column('name', String(20)))
    # 处理 INDEX_STATS_DATA 中的列
    if table_structure == INDEX_STATS_DATA:
        for col_name, col_info in table_structure['columns'].items():
            column = Column(col_name, col_info['type'])
            columns.append(column)
    else:
        for col in table_structure['columns']:
            if isinstance(col, dict) and 'name' in col and 'type' in col:
                column = Column(col['name'], col['type'])
                columns.append(column)
            elif isinstance(col, str):
                # 假设默认类型为 Float，对于 date 列使用 Date 类型
                if col == 'date':
                    continue  # 已经添加过 date 列
                elif col == 'code':
                    continue  # 已经添加过 code 列
                else:
                    column = Column(col, Float)
                    columns.append(column)
            else:
                logging.error(f"Invalid column definition: {col}")
    if columns:
        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)
        return table
    else:
        logging.error(f"No valid columns found for table {table_name}, skipping table creation.")
        return None


__author__ = 'hqm'
__date__ = '2025/03/25'


# 过滤价格，如果没有基本上是退市了。
def is_open(price):
    return not np.isnan(price)


# 读取当天股票数据
class index_data(metaclass=singleton_type):
    def __init__(self, date):
        try:
            self.data = fetch_indexs(date)
        except Exception as e:
            logging.error(f"index_data，读取当天股票数据，处理异常：{e}")

    def get_data(self):
        return self.data


# 读取当天股票数据
def fetch_indexs(date):
    try:
        data = index_zh_a_spot_em()
        # data = get_index_data(date)
        if data is None or len(data.index) == 0:
            return None
        if date is None:
            data.insert(0, 'date', datetime.now().strftime("%Y-%m-%d"))
        else:
            data.insert(0, 'date', date.strftime("%Y-%m-%d"))
        data.columns = list(tbs.TABLE_CN_INDEX_SPOT['columns'])
        # data = data.loc[data['code'].apply(is_a_stock)].loc[data['new_price'].apply(is_open)]
        data = data.loc[data['new_price'].apply(is_open)]
        return data
    except Exception as e:
        logging.error(f"fetch_indexs，读取当天股票数据，处理异常：{e}")
    return None


# 读取指数历史数据
class index_hist_data(metaclass=singleton_type):
    def __init__(self, date=None, stocks=None, workers=16):
        if stocks is None:
            _subset = index_data(date).get_data()[list(tbs.TABLE_CN_INDEX_FOREIGN_KEY['columns'])]
            # print(f'{_subset}')
            stocks = [tuple(x) for x in _subset.values]
        if stocks is None:
            self.data = None
            return
        date_start, is_cache = trd.get_trade_hist_interval(stocks[0][0])  # 提高运行效率，只运行一次
        # print(f'{date_start}')
        _data = {}
        try:
            # max_workers是None还是没有给出，将默认为机器cup个数*5
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_stock = {executor.submit(fetch_index_hist, stock[1], "daily", date_start, "20500101", ""): stock for stock
                                   in stocks}
                # print(f'{future_to_stock}')
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        __data = future.result()
                        # print(f'{__data}')
                        if __data is not None:
                            _data[stock] = __data
                    except Exception as e:
                        logging.error(f"index_hist_data读取指数历史数据处理异常：{stock[1]}代码{e}")
        except Exception as e:
            logging.error(f"index_hist_data读取指数历史数据处理异常：{e}")
        if not _data:
            self.data = None
        else:
            self.data = _data

    def get_data(self):
        return self.data


def index_zh_a_spot_em() -> pd.DataFrame:
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
        "fields": "f12,f14,f2,f4,f3,f5,f6,f18,f17,f15,f16",
        "_": "1623833739532",
    }
    try:
        # 发送第一次请求获取总数据量和 page_size
        r = requests.get(url, params=params)
        r.raise_for_status()
        data_json = r.json()
        # print(f'{data_json}')
        total = data_json["data"]["total"]
        # 获取 page_size
        page_size = len(data_json["data"]["diff"])
        total_pages = (total + page_size - 1) // page_size

        all_data = []
        for page in range(1, total_pages + 1):
            params["pn"] = str(page)
            params["pz"] = str(page_size)
            r = requests.get(url, params=params)
            r.raise_for_status()
            data_json = r.json()
            if data_json["data"]["diff"]:
                all_data.extend(data_json["data"]["diff"])

        if not all_data:
            return pd.DataFrame()

        temp_df = pd.DataFrame(all_data)
        temp_df.columns = [
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "代码",
            "名称",
            "最高",
            "最低",
            "今开",
            "昨收"
        ]
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "今开",
                "最高",
                "最低",
                "昨收"]
        ]
        # 批量转换数据类型
        numeric_cols = [
            "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "最高", "最低", "今开", "昨收"]
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        # print(f"temp_df: {temp_df}")

        return temp_df
    except requests.RequestException as e:
        print(f"请求出错: {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"解析数据出错: {e}")
        return pd.DataFrame()




def fetch_index_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code_id FROM cn_index_info where code_str = {symbol}", conn)
    conn.close()
    code_id_dict = stock_df
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    # print(f"Fetching historical data for symbol {symbol} with params: {params}")
    r = requests.get(url, params=params)
    data_json = r.json()
    # print(f'{data_json}')
    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame(
        [item.split(",") for item in data_json["data"]["klines"]]
    )
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ]
    temp_df.index = pd.to_datetime(temp_df["日期"])
    temp_df.reset_index(inplace=True, drop=True)

    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
    temp_df["最高"] = pd.to_numeric(temp_df["最高"])
    temp_df["最低"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])

    return temp_df


@lru_cache()
def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    code_id_dict = {}

    # 定义不同市场的参数配置
    # market_configs = [
    #     {
    #         "market_id": 1,
    #         "fs": "m:1 t:2,m:1 t:23",
    #         "column_name": "sh_code"
    #     },
    #     {
    #         "market_id": 0,
    #         "fs": "m:0 t:6,m:0 t:80",
    #         "column_name": "sz_code"
    #     },
    #     {
    #         "market_id": 0,
    #         "fs": "m:0 t:81 s:2048",
    #         "column_name": "bj_code"
    #     }
    # ]

    for config in market_configs:
        # 先获取总数据量和 page_size
        params = {
            "pn": "1",
            "pz": "1000",  # 可以设置一个较大的值来获取尽可能多的数据用于确定 page_size
            "po": "1",
            "np": "3",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "b:MK0010",
            "fields": "f12",
            "_": "1623833739532",
        }
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            data_json = r.json()
            total = data_json["data"]["total"]
            # 获取 page_size
            page_size = len(data_json["data"]["diff"])
            total_pages = (total + page_size - 1) // page_size

            all_data = []
            for page in range(1, total_pages + 1):
                params["pn"] = str(page)
                params["pz"] = str(page_size)
                r = requests.get(url, params=params)
                r.raise_for_status()
                data_json = r.json()
                if data_json["data"]["diff"]:
                    all_data.extend(data_json["data"]["diff"])

            if all_data:
                temp_df = pd.DataFrame(all_data)
                temp_df[config["column_name"]] = temp_df["f12"]
                temp_df[f'{config["column_name"][:2]}_id'] = config["market_id"]
                code_id_dict.update(dict(zip(temp_df[config["column_name"]], temp_df[f'{config["column_name"][:2]}_id'])))
        except requests.RequestException as e:
            print(f"请求出错: {e}")
        except KeyError as e:
            print(f"解析数据出错: {e}")

    return code_id_dict


def prepare(date):
    engine = mdb.engine()
    if engine is None:
        logging.error("数据库连接失败，无法继续执行。")
        return
    try:
        indexs_data = index_hist_data(date=date).get_data()
        if indexs_data is None:
            return
        results = run_check(indexs_data, date=date)
        if results is None:
            return

        table_name = tbs.TABLE_CN_INDEX_INDICATORS['name']
        # 检查并创建表
        create_table_from_structure(engine, table_name, INDEX_STATS_DATA)

        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_INDEX_FOREIGN_KEY['columns'])
        dataKey.columns = _columns

        dataVal = pd.DataFrame(results.values())
        dataVal.drop('date', axis=1, inplace=True)  # 删除日期字段，然后和原始数据合并。

        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        date_str = date.strftime("%Y-%m-%d")
        if date_str != data.iloc[0]['date']:
            data['date'] = date_str

        # 处理NaN值
        data = data.where(pd.notnull(data), None)

        print(f'{data}')
        # 先删除当天的旧数据
        with engine.begin() as conn:
            delete_sql = text(f"DELETE FROM `{table_name}` WHERE `date` = :date")
            # 传入日期字符串而不是 datetime 对象
            conn.execute(delete_sql, {"date": date_str})

        # 手动指定 date 列的数据类型为 SQLAlchemy 的 Date 类型
        cols_type = {'date': Date}
        # 分批插入数据
        chunksize = 1000
        mdb.insert_db_from_df(data, table_name, cols_type, False, '`date`, `code`')
    except Exception as e:
        logging.error(f"indicators_data_daily_job.prepare处理异常：{e}")


def run_check(stocks, date=None, workers=40):
    data = {}
    columns = ['date', 'code'] + list(INDEX_STATS_DATA['columns'].keys())
    data_column = columns

    preprocessed_stocks = {}
    for key, df in stocks.items():
        if not df.empty:
            new_df = df.copy()
            new_df['code'] = key[0]
            new_df = new_df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交额': 'amount',
                '成交量': 'volume',
                '换手率': 'turnover',
                '涨跌幅': 'quote_change',
                '涨跌额': 'ups_downs'
            })
            preprocessed_stocks[key] = new_df
        else:
            preprocessed_stocks[key] = df

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(idr.get_indicator, k, preprocessed_stocks[k], data_column, date=date): (k, date) for k in preprocessed_stocks}
            for future in concurrent.futures.as_completed(futures):
                stock, current_date = futures[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                except Exception as e:
                    logging.error(f"indicators_index_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
    except Exception as e:
        logging.error(f"indicators_index_data_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data


# 对每日指标数据，进行筛选。将符合条件的。二次筛选出来。
# 只是做简单筛选
def guess_buy(date):
    engine = mdb.engine()
    if engine is None:
        logging.error("数据库连接失败，无法继续执行。")
        return
    try:
        _table_name = tbs.TABLE_CN_INDEX_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = tuple(tbs.TABLE_CN_INDEX_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)

        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and
                `kdjj` <= 0 and `rsi_6` <= 30 and
                `cci` <= -130 and `rsi_12` <= 45 and `close` <= `boll_lb` and
                ABS(`wr_6`) >= 90 and ABS(`wr_10`) >= 90'''

        data = pd.read_sql(sql=sql, con=engine)
        data = data.drop_duplicates(subset="code", keep="last")

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_INDEX_INDICATORS_BUY['name']
        # 检查并创建表
        create_table_from_structure(engine, table_name, tbs.TABLE_CN_INDEX_INDICATORS_BUY)

        _columns_backtest = tuple(tbs.TABLE_CN_INDEX_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

        # 先删除当天的旧数据
        date_str = date.strftime("%Y-%m-%d")
        with engine.begin() as conn:
            delete_sql = text(f"DELETE FROM `{table_name}` WHERE `date` = :date")
            conn.execute(delete_sql, {"date": date_str})

        # 手动指定 date 列的数据类型为 SQLAlchemy 的 Date 类型
        cols_type = {'date': Date}  # 修改这里
        # 分批插入数据
        chunksize = 1000
        mdb.insert_db_from_df(data, table_name, cols_type, False, '`date`, `code`')
        logging.info(f"成功插入 {date_str} 的 {len(data)} 条指数买入指标数据")
    except Exception as e:
        logging.error(f"indicators_etf_data_daily_job.guess_buy处理异常：{e}")


# 设置卖出数据。
def guess_sell(date):
    engine = mdb.engine()
    if engine is None:
        logging.error("数据库连接失败，无法继续执行。")
        return
    try:
        _table_name = tbs.TABLE_CN_INDEX_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = tuple(tbs.TABLE_CN_INDEX_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)

        date_str = date.strftime("%Y-%m-%d")
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date_str}' and
                `kdjj` >= 90 and `rsi_6` >= 65 and
                `cci` >= 180 and `rsi_12` >= 65 and `close` >= `boll_ub` and
                ABS(`wr_6`) <= 5'''

        data = pd.read_sql(sql=sql, con=engine)
        data = data.drop_duplicates(subset="code", keep="last")

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_INDEX_INDICATORS_SELL['name']
        # 检查并创建表
        create_table_from_structure(engine, table_name, tbs.TABLE_CN_INDEX_INDICATORS_SELL)

        _columns_backtest = tuple(tbs.TABLE_CN_INDEX_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)], ignore_index=True)

        # 先删除当天的旧数据
        date_str = date.strftime("%Y-%m-%d")
        with engine.begin() as conn:
            delete_sql = text(f"DELETE FROM `{table_name}` WHERE `date` = :date")
            conn.execute(delete_sql, {"date": date_str})

        # 手动指定 date 列的数据类型为 SQLAlchemy 的 Date 类型
        cols_type = {'date': Date}  # 修改这里
        # 分批插入数据
        chunksize = 1000
        mdb.insert_db_from_df(data, table_name, cols_type, False, '`date`, `code`')
        logging.info(f"成功插入 {date_str} 的 {len(data)} 条指数卖出指标数据")
    except Exception as e:
        logging.error(f"indicators_index_data_daily_job.guess_sell处理异常：{e}")


def main():
    # 配置日志
    logging.basicConfig(
        filename='indicators_index_data_daily_job.log',
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
        print("单个日期：python indicators_index_data_daily_job.py 2024-04-02")
        print("日期区间：python indicators_index_data_daily_job.py 2024-04-02 2024-04-05")
        return

    for date in dates:
        prepare(date)
        guess_buy(date)
        guess_sell(date)


# main函数入口
if __name__ == '__main__':
    main()
    