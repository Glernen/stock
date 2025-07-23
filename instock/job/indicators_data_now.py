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
import time
import datetime
import mysql.connector
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import pandas_market_calendars as mcal
from mysql.connector import Error
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset
import indicators_data_daily as indicators_data_daily
import threeday_indicators as threeday_indicators
import market_sentiment_a as market_sentiment_a
import industry_data as industry_data
import industry_sentiment_a as industry_sentiment_a
import indicators_strategy_buy as indicators_strategy_buy
import buy_20250425 as buy_20250425
import basic_data as basic_data


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





"""
定义公共函数：
create_table_if_not_exists(table_name)：检查数据表是否存在，如果不存在，则创建数据库并添加索引
同步表结构(conn, table_name, data_columns)： 动态检查并自动添加表中缺失的字段
sql语句生成器(table_name,data)：带入参数数据表名和数据，生成数据插入语句
execute_raw_sql(sql,params)：执行插入数据表
"""


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



def sql_batch_generator(table_name, data, batch_size=500):
    """通用批量SQL生成器"""
    # if 'code' in data.columns and 'code_int' not in data.columns:
    #     data.insert(0, 'code_int', data['code'].astype(int))

    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'code_int']
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




def execute_raw_sql(sql, params=None, max_query_size=1024*1024, batch_size=500):
    """改进后的SQL执行函数，解决Commands out of sync问题"""
    connection = DBManager.get_new_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor(buffered=True)  # 使用缓冲游标
        statements = [s.strip() for s in sql.split(';') if s.strip()]

        for statement in statements:
            try:
                # 执行当前语句
                cursor.execute(statement)
                # 显式消费所有结果集（关键修复）
                while True:
                    if cursor.with_rows:
                        cursor.fetchall()  # 消费结果集
                    if not cursor.nextset():
                        break
            except Error as e:
                # 更详细的错误日志
                print(f"执行失败: {statement[:100]}... | 错误类型: {type(e).__name__} | 错误详情: {str(e)}")
                connection.rollback()
                return False
            except Exception as ex:
                print(f"未知错误: {ex}")
                connection.rollback()
                return False
        connection.commit()  # 所有语句执行成功后提交
        return True
    except Error as e:
        print(f"数据库错误: {e}")
        connection.rollback()
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("数据库连接已安全关闭")  # 调试日志




def main():

    # 实时股票 OK
    # stock_zh_a_spot_em()

    # index_zh_a_spot_em()

    # etf_spot_em()

    # industry_zh_a_spot_em()

    #基础数据脚本会将实时数据回填到实时表、基础信息表、历史数据表
    # basic_data.main()


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


    indicators_data_daily.main()

    threeday_indicators.main()

    market_sentiment_a.main()

    industry_data.main()

    industry_sentiment_a.main()

    indicators_strategy_buy.main()

    buy_20250425.main()




# main函数入口
if __name__ == '__main__':
    main()
