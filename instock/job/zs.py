#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
from datetime import datetime, timedelta
import sys
from instock.lib.database import mdb
from instock.core.tablestructure import tbs
from instock.core.stockfetch import fetch_index_hist
import instock.core.indicator.calculate_indicator as idr

__author__ = 'hqm'
__date__ = '2025/03/25'

# 目标指数列表
TARGET_INDICES = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000300": "沪深300",
    "000905": "中证2000"
}

def prepare(date):
    date_str = date.strftime("%Y%m%d")
    for symbol, name in TARGET_INDICES.items():
        try:
            # 获取指数历史数据
            df = fetch_index_hist(
                symbol=symbol,
                start_date=date_str,
                end_date=date_str
            )

            if df.empty:
                logging.warning(f"未获取到 {name} 数据")
                continue

            # 使用 idr.get_indicator 获取指标数据
            columns = list(tbs.TABLE_CN_INDEX_INDICATORS['columns'])
            columns.insert(0, 'code')
            columns.insert(0, 'date')
            indicators = idr.get_indicator(symbol, df, columns, date=date_str)

            if indicators is None:
                logging.warning(f"未获取到 {name} 的指标数据")
                continue

            # 保存基础数据
            save_index_spot_data(symbol, name, df)

            # 保存指标数据
            save_index_indicators(symbol, indicators)

            # 生成买卖信号
            generate_signals(symbol, indicators)

            logging.info(f"成功处理 {name} 数据")

        except Exception as e:
            logging.error(f"处理 {name} 时出错: {str(e)}")

def save_index_spot_data(symbol: str, name: str, data: pd.DataFrame):
    """保存指数基础行情数据"""
    table_name = tbs.TABLE_CN_INDEX_FOREIGN_KEY['name']
    columns = tbs.TABLE_CN_INDEX_FOREIGN_KEY['columns']

    # 构建基础数据
    spot_data = data[['date', 'close']].copy()
    spot_data['date'] = spot_data['date'].dt.strftime("%Y-%m-%d")
    spot_data['code'] = symbol
    spot_data['name'] = name
    spot_data['index_type'] = get_index_type(symbol)

    # 分批插入
    spot_data.to_sql(
        name=table_name,
        con=mdb.engine(),
        if_exists='append',
        index=False,
        chunksize=1000
    )

def save_index_indicators(symbol: str, indicators: pd.DataFrame):
    """保存指数技术指标"""
    table_name = tbs.TABLE_CN_INDEX_INDICATORS['name']
    columns = tbs.TABLE_CN_INDEX_INDICATORS['columns']

    # 转换日期格式
    indicators['date'] = indicators['date'].dt.strftime("%Y-%m-%d")
    indicators['code'] = symbol

    # 过滤有效列
    indicators = indicators[columns]

    # 分批插入
    indicators.to_sql(
        name=table_name,
        con=mdb.engine(),
        if_exists='append',
        index=False,
        chunksize=1000
    )

def generate_signals(symbol: str, indicators: pd.DataFrame):
    """生成买卖信号"""
    # 买入信号条件
    buy_conditions = (
        (indicators['J'] < 20) &
        (indicators['RSI_6'] < 30) &
        (indicators['CCI'] < -100) &
        (indicators['close'] < indicators['BOLL_LB'])
    )

    # 卖出信号条件
    sell_conditions = (
        (indicators['J'] > 80) &
        (indicators['RSI_6'] > 70) &
        (indicators['CCI'] > 100) &
        (indicators['close'] > indicators['BOLL_UB'])
    )

    # 构建信号数据
    buy_data = indicators[buy_conditions].copy()
    sell_data = indicators[sell_conditions].copy()

    # 保存买入信号
    if not buy_data.empty:
        buy_data['buy_signal'] = 1
        buy_data['kdj_condition'] = (buy_data['J'] < 20).astype(int)
        buy_data['rsi_condition'] = (buy_data['RSI_6'] < 30).astype(int)
        buy_data['cci_condition'] = (buy_data['CCI'] < -100).astype(int)
        buy_data['boll_condition'] = (buy_data['close'] < buy_data['BOLL_LB']).astype(int)
        buy_data.to_sql(
            name=tbs.TABLE_CN_INDEX_INDICATORS_BUY['name'],
            con=mdb.engine(),
            if_exists='append',
            index=False,
            chunksize=1000
        )

    # 保存卖出信号
    if not sell_data.empty:
        sell_data['sell_signal'] = 1
        sell_data['kdj_condition'] = (sell_data['J'] > 80).astype(int)
        sell_data['rsi_condition'] = (sell_data['RSI_6'] > 70).astype(int)
        sell_data['cci_condition'] = (sell_data['CCI'] > 100).astype(int)
        sell_data['boll_condition'] = (sell_data['close'] > sell_data['BOLL_UB']).astype(int)
        sell_data.to_sql(
            name=tbs.TABLE_CN_INDEX_INDICATORS_SELL['name'],
            con=mdb.engine(),
            if_exists='append',
            index=False,
            chunksize=1000
        )

def get_index_type(symbol: str) -> str:
    """获取指数类型"""
    type_map = {
        "000001": "综合指数",
        "399001": "成份指数",
        "399006": "板块指数",
        "000300": "策略指数",
        "000905": "规模指数"
    }
    return type_map.get(symbol, "未知类型")

# 对每日指标数据，进行筛选。将符合条件的。二次筛选出来。
# 只是做简单筛选
def guess_buy(date):
    try:
        _table_name = tbs.TABLE_CN_INDEX_INDICATORS['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        _columns = tuple(tbs.TABLE_CN_INDEX_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)

        date_str = date.strftime("%Y-%m-%d")
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date_str}' and
                `kdjj` <= 0 and `rsi_6` <= 30 and
                `cci` <= -130 and `rsi_12` <= 45 and `close` <= `boll_lb` and
                ABS(`wr_6`) >= 90 and ABS(`wr_10`) >= 90'''

        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_INDEX_INDICATORS_BUY['name']
        _columns_backtest = tuple(tbs.TABLE_CN_INDEX_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)], ignore_index=True)

        # 先删除当天的旧数据
        with mdb.engine().begin() as conn:
            delete_sql = f"DELETE FROM `{table_name}` WHERE `date` = '{date_str}'"
            conn.execute(delete_sql)

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)
        logging.info(f"成功插入 {date_str} 的 {len(data)} 条指数买入指标数据")

    except Exception as e:
        logging.error(f"indicators_index_data_daily_job.guess_buy处理异常：{e}")


# 设置卖出数据。
def guess_sell(date):
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

        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")

        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_INDEX_INDICATORS_SELL['name']
        _columns_backtest = tuple(tbs.TABLE_CN_INDEX_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)], ignore_index=True)

        # 先删除当天的旧数据
        with mdb.engine().begin() as conn:
            delete_sql = f"DELETE FROM `{table_name}` WHERE `date` = '{date_str}'"
            conn.execute(delete_sql)

        # 分批插入数据
        chunksize = 1000  # 可以根据实际情况调整
        data.to_sql(table_name, mdb.engine(), if_exists='append', index=False, chunksize=chunksize)
        logging.info(f"成功插入 {date_str} 的 {len(data)} 条指数卖出指标数据")

    except Exception as e:
        logging.error(f"indicators_index_data_daily_job.guess_sell处理异常：{e}")

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

    