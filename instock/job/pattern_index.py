#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import os.path
import sys

# 添加项目路径到环境变量
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


# import pandas_ta as ta


########################################################################

def identify_hammer(df, open_col='open', high_col='high',
                    low_col='low', close_col='close',
                    min_lower_shadow_ratio=1.5, max_upper_shadow_ratio=0.1):
    """
    识别锤子线形态（包括阳锤子线和阴锤子线）

    参数:
    df: DataFrame，包含指数OHLCV数据
    open_col: 开盘价列名
    high_col: 最高价列名
    low_col: 最低价列名
    close_col: 收盘价列名
    min_lower_shadow_ratio: 下影线长度至少是实体的倍数
    max_upper_shadow_ratio: 上影线长度最多是实体的倍数

    返回:
    DataFrame，增加两列：
        bullish_hammer - 阳锤子线标识（1表示是，0表示否）
        bearish_hammer - 阴锤子线标识（1表示是，0表示否）
    """
    # 确保输入的DataFrame包含必要的列
    required_cols = [open_col, high_col, low_col, close_col]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"输入的DataFrame缺少必要的列: {required_cols}")

    # 复制数据，避免修改原始DataFrame
    result = df.copy()

    # 计算实体大小
    body = np.abs(result[close_col] - result[open_col])

    # 计算上下影线长度
    lower_shadow = np.where(
        result[open_col] < result[close_col],
        result[open_col] - result[low_col],  # 阳线情况
        result[close_col] - result[low_col]  # 阴线情况
    )

    upper_shadow = np.where(
        result[open_col] < result[close_col],
        result[high_col] - result[close_col],  # 阳线情况
        result[high_col] - result[open_col]  # 阴线情况
    )

    # 实体长度（绝对值）
    body_size = np.abs(result[close_col] - result[open_col])

    # 识别阳锤子线条件
    bullish_condition = (
            (result[close_col] > result[open_col]) &  # 阳线
            (lower_shadow >= body_size * min_lower_shadow_ratio) &  # 长下影线
            (upper_shadow <= body_size * max_upper_shadow_ratio)  # 短上影线
    )

    # 识别阴锤子线条件
    bearish_condition = (
            (result[close_col] < result[open_col]) &  # 阴线
            (lower_shadow >= body_size * min_lower_shadow_ratio) &  # 长下影线
            (upper_shadow <= body_size * max_upper_shadow_ratio)  # 短上影线
    )

    # 添加识别结果
    result['bullish_hammer'] = bullish_condition.astype(int)
    result['bearish_hammer'] = bearish_condition.astype(int)

    return result


########################################################################

def get_latest_trading_date():
    """获取最新的交易日期"""
    today_int = int(datetime.datetime.now().strftime("%Y%m%d"))
    sql = f"""
    SELECT MAX(date_int) AS max_date 
    FROM kline_index 
    WHERE date_int <= {today_int}
    """
    result = DBManager.query_sql(sql)
    return result.iloc[0]['max_date'] if not result.empty else None


def get_stock_data(date_int):
    """获取指定日期的指数数据"""
    sql = f"""
    SELECT *
    FROM kline_index 
    WHERE date_int = {date_int}
    """
    return DBManager.query_sql(sql)


def process_stock_data(stock_data):
    """处理指数数据并识别锤子线形态"""
    # 转换为DataFrame
    df = pd.DataFrame(stock_data)
    if df.empty:
        return df

    # 识别锤子线
    df = identify_hammer(df)

    # 添加时间戳
    # df['create_time'] = datetime.datetime.now()
    # df['update_time'] = datetime.datetime.now()

    return df


########################################################################
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
                charset=db_charset,
                use_pure=True  # 强制使用纯Python实现
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

    @staticmethod
    def query_sql(sql: str, params=None):
        """执行查询SQL并返回DataFrame"""
        connection = DBManager.get_new_connection()
        if connection:
            try:
                # 直接使用SQL字符串查询，不使用参数化
                return pd.read_sql_query(sql, connection)
            except Error as e:
                print(f"Error while querying SQL: {e}")
                return pd.DataFrame()
            finally:
                if connection.is_connected():
                    connection.close()
        else:
            return pd.DataFrame()


def sql_batch_generator(table_name, data, batch_size=500):
    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'code_int']

    # 修改点：为每个非唯一键字段生成条件更新语句
    update_clauses = []
    for col in data.columns:
        if col not in unique_keys:
            # 只有当新值不为NULL时才更新该字段
            update_clauses.append(
                f"`{col}` = COALESCE(VALUES(`{col}`), `{col}`)"
            )
    update_clause = ', '.join(update_clauses)

    batches = []
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        value_rows = []

        for row in batch.itertuples(index=False):
            values = []
            for item in row:
                if pd.isna(item) or item in ['-', '']:
                    values.append("NULL")
                elif isinstance(item, (datetime.date, datetime.datetime)):
                    values.append(f"'{item.strftime('%Y-%m-%d %H:%M:%S')}'")
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
                time.sleep(2 ** attempt)  # 指数退避
            finally:
                if conn and conn.is_connected():
                    cursor.close()
                    conn.close()


def save_to_database(df):
    """将结果保存到数据库"""
    if df.empty:
        print("没有数据需要保存")
        return

    # 只保留识别到锤子线形态的记录
    mask = (df['bullish_hammer'] == 1) | (df['bearish_hammer'] == 1)
    filtered_df = df[mask]

    if filtered_df.empty:
        print("没有识别到锤子线形态的数据需要保存")
        return

    # 重命名列以匹配数据库表
    filtered_df.rename(columns={
        'bullish_hammer': '阳锤子线',
        'bearish_hammer': '阴锤子线'
    }, inplace=True)

    # 生成批量SQL
    sql_batches = sql_batch_generator('pattern_index', filtered_df, 6000)

    # 执行批量插入
    execute_batch_sql(sql_batches)

    print(f"成功保存 {len(filtered_df)} 条记录到数据库")


def main():
    # 获取最新交易日期
    latest_date = get_latest_trading_date()
    if not latest_date:
        print("未找到有效的交易日期")
        return

    print(f"处理交易日期: {latest_date}")

    # 获取该日期的指数数据
    stock_data = get_stock_data(latest_date)
    if stock_data.empty:
        print(f"未找到 {latest_date} 的指数数据")
        return

    print(f"获取到 {len(stock_data)} 条指数数据")

    # 处理数据并识别形态
    result_df = process_stock_data(stock_data)

    if result_df.empty:
        print("未识别到任何锤子线形态")
        return

    # 统计结果
    bullish_count = result_df['bullish_hammer'].sum()
    bearish_count = result_df['bearish_hammer'].sum()
    print(f"识别到 {bullish_count} 只指数出现阳锤子线形态")
    print(f"识别到 {bearish_count} 只指数出现阴锤子线形态")

    # 保存到数据库
    save_to_database(result_df)


if __name__ == "__main__":
    main()