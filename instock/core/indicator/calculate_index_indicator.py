#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import numpy as np
import talib as tl

__author__ = 'hqm'
__date__ = '2025/03/26'


def get_indicators(data, end_date=None, threshold=120, calc_threshold=None):
    try:
        isCopy = False
        if end_date is not None:
            mask = (data['date'] <= end_date)
            data = data.loc[mask]
            isCopy = True
        if calc_threshold is not None:
            data = data.tail(n=calc_threshold)
            isCopy = True

        if isCopy:
            data = data.copy()

        # import stockstats
        # test = data.copy()
        # test = stockstats.StockDataFrame.retype(test)  # 验证计算结果

        with np.errstate(divide='ignore', invalid='ignore'):

            # macd
            data.loc[:, 'macd'], data.loc[:, 'macds'], data.loc[:, 'macdh'] = tl.MACD(
                data['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            data['macd'].values[np.isnan(data['macd'].values)] = 0.0
            data['macds'].values[np.isnan(data['macds'].values)] = 0.0
            data['macdh'].values[np.isnan(data['macdh'].values)] = 0.0

            # kdjk
            data.loc[:, 'kdjk'], data.loc[:, 'kdjd'] = tl.STOCH(
                data['high'].values, data['low'].values, data['close'].values, fastk_period=9,
                slowk_period=5, slowk_matype=1, slowd_period=5, slowd_matype=1)
            data['kdjk'].values[np.isnan(data['kdjk'].values)] = 0.0
            data['kdjd'].values[np.isnan(data['kdjd'].values)] = 0.0
            data.loc[:, 'kdjj'] = 3 * data['kdjk'].values - 2 * data['kdjd'].values

            # boll
            data.loc[:, 'boll_ub'], data.loc[:, 'boll'], data.loc[:, 'boll_lb'] = tl.BBANDS(
                data['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            data['boll_ub'].values[np.isnan(data['boll_ub'].values)] = 0.0
            data['boll'].values[np.isnan(data['boll'].values)] = 0.0
            data['boll_lb'].values[np.isnan(data['boll_lb'].values)] = 0.0

            # rsi
            data.loc[:, 'rsi'] = tl.RSI(data['close'].values, timeperiod=14)
            data['rsi'].values[np.isnan(data['rsi'].values)] = 0.0
            data.loc[:, 'rsi_6'] = tl.RSI(data['close'].values, timeperiod=6)
            data['rsi_6'].values[np.isnan(data['rsi_6'].values)] = 0.0
            data.loc[:, 'rsi_12'] = tl.RSI(data['close'].values, timeperiod=12)
            data['rsi_12'].values[np.isnan(data['rsi_12'].values)] = 0.0

            # wr
            data.loc[:, 'wr_6'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=6)
            data['wr_6'].values[np.isnan(data['wr_6'].values)] = 0.0
            data.loc[:, 'wr_10'] = tl.WILLR(data['high'].values, data['low'].values, data['close'].values, timeperiod=10)
            data['wr_10'].values[np.isnan(data['wr_10'].values)] = 0.0

            # cci
            data.loc[:, 'cci'] = tl.CCI(data['high'].values, data['low'].values, data['close'].values, timeperiod=14)
            data['cci'].values[np.isnan(data['cci'].values)] = 0.0

        if threshold is not None:
            data = data.tail(n=threshold).copy()
        return data
    except Exception as e:
        logging.error(f"calculate_index_indicator.get_index_indicators处理异常：{data['code']}代码{e}")
    return None


def get_indicator(code_name, data, stock_column, date=None, calc_threshold=90):
    try:
        print(f'code_name:{code_name}')
        print(f'stock_column:{stock_column}')

        code = code_name[1]

        if date is None:
            end_date = code_name[0]
        else:
            end_date = date.strftime("%Y-%m-%d")

        # 设置返回数组。
        stock_data_list = [end_date, code]
        columns_num = len(stock_column) - 2

        # 如果 data 是字典，将其转换为 DataFrame
        if isinstance(data, dict):
            data = pd.DataFrame(data)

        # 增加空判断，如果是空返回 0 数据。
        if len(data.index) <= 1:
            for i in range(columns_num):
                stock_data_list.append(0)
        else:
            idr_data = get_index_indicators(data, end_date=end_date, threshold=1, calc_threshold=calc_threshold)

            # 增加空判断，如果是空返回 0 数据。
            if idr_data is None:
                for i in range(columns_num):
                    stock_data_list.append(0)
            else:
                # 初始化统计类
                for i in range(columns_num):
                    # 将数据的最后一个返回。
                    column_name = stock_column[i + 2]
                    if column_name not in idr_data.columns:
                        logging.error(f"数据中缺少 '{column_name}' 列: {code}")
                        stock_data_list.append(0)
                        continue
                    try:
                        tmp_val = idr_data[column_name].tail(1).values[0]
                        # 解决值中存在 INF NaN 问题。
                        if np.isinf(tmp_val) or np.isnan(tmp_val):
                            stock_data_list.append(0)
                        else:
                            stock_data_list.append(tmp_val)
                    except IndexError:
                        logging.error(f"无法从 {column_name} 列获取数据: {code}")
                        stock_data_list.append(0)

        # 检查 stock_data_list 是否都是标量值
        if all(isinstance(val, (int, float)) for val in stock_data_list):
            return pd.Series(stock_data_list, index=stock_column)
        else:
            logging.error(f"stock_data_list 包含非标量值: {code}")
            return None

    except Exception as e:
        logging.error(f"calculate_indicator.get_indicator处理异常：{code}代码{e}")
    return None