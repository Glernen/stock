#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys

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
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset


def get_kline_index_from_realtime_df():
    """从realtime_stock_df表查询数据"""
    sql = """
    SELECT 
        rss.date,
        rss.date_int,
        rss.code,
        rss.code_int,
        rss.name,
        rss.`开盘价` AS open,
        rss.`最高价` AS high,
        rss.`最低价` AS low,
        rss.`收盘价` AS close,
        rss.`成交量(手)` AS volume,
        rss.`换手率(%)` AS turnoverrate
    FROM 
        `realtime_index_df` rss
    WHERE
        rss.date_int = (SELECT MAX(date_int) FROM `realtime_index_df`)
        AND rss.code IN (
            SELECT code 
            FROM `basic_info_index` 
            WHERE `参与指标计算` = 1
        );
    """
    print("正在从realtime_index_df查询指数数据...")
    df = DBManager.query_sql(sql)
    print(f"查询完成，获取到 {len(df)} 条记录")
    return df


def get_kline_index_from_realtime_sina():
    """从realtime_stock_sina表查询数据（添加turnoverrate字段）"""
    sql = """
    SELECT 
        rss.date,
        rss.date_int,
        rss.code,
        rss.code_int,
        rss.name,
        rss.`开盘价` AS open,
        rss.`最高价` AS high,
        rss.`最低价` AS low,
        rss.`收盘价` AS close,
        rss.`成交量(手)` AS volume
    FROM 
        `realtime_index_sina` rss
    WHERE
        rss.date_int = (SELECT MAX(date_int) FROM `realtime_index_sina`)
        AND rss.code IN (
            SELECT code 
            FROM `basic_info_index` 
            WHERE `参与指标计算` = 1
        );
    """
    print("正在从realtime_index_sina查询股票数据...")
    df = DBManager.query_sql(sql)
    print(f"查询完成，获取到 {len(df)} 条记录")
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
                return pd.read_sql(sql, connection)
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
                time.sleep(2 ** attempt)  # 指数退避
            finally:
                if conn and conn.is_connected():
                    cursor.close()
                    conn.close()


def cleanup_old_kline_data():
    """清理kline_index表中超过90个交易日的数据"""
    print("开始清理超过90个交易日的旧数据...")

    # 获取最近90个交易日
    xshg = mcal.get_calendar('XSHG')  # 上海证券交易所日历
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=180)  # 确保覆盖90个交易日
    schedule = xshg.schedule(start_date=start_date, end_date=end_date)
    trading_days = schedule.index.date

    if len(trading_days) > 90:
        keep_days = trading_days[-90:]
    else:
        keep_days = trading_days

    # 转换为整数格式
    keep_days_int = [int(day.strftime('%Y%m%d')) for day in keep_days]

    # 构建SQL语句
    keep_days_str = ','.join(map(str, keep_days_int))
    delete_sql = f"""
    DELETE FROM `kline_index`
    WHERE date_int NOT IN ({keep_days_str});
    """

    # 执行删除
    try:
        DBManager.execute_sql(delete_sql)
        print(f"已清理超过90个交易日的旧数据，保留日期: {keep_days[0]} 至 {keep_days[-1]}")
    except Exception as e:
        print(f"清理旧数据时出错: {e}")


def main():
    start_time = time.time()
    total_rows = 0

    # 第一步：从realtime_stock_df表获取数据并插入
    df_df = get_kline_index_from_realtime_df()
    if not df_df.empty:
        # 生成批量SQL
        sql_batches_df = sql_batch_generator(
            table_name='kline_index',
            data=df_df,
            batch_size=6000
        )
        # 执行批量插入
        execute_batch_sql(sql_batches_df)
        total_rows += len(df_df)
        print(f"[Success]realtime_index_df数据写入完成，数据量：{len(df_df)}")
    else:
        print("未获取到realtime_index_df数据")

    # 第二步：从realtime_stock_sina表获取数据并插入
    df_sina = get_kline_index_from_realtime_sina()
    if not df_sina.empty:
        # 生成批量SQL
        sql_batches_sina = sql_batch_generator(
            table_name='kline_index',
            data=df_sina,
            batch_size=6000
        )
        # 执行批量插入
        execute_batch_sql(sql_batches_sina)
        total_rows += len(df_sina)
        print(f"[Success]realtime_index_sina数据写入完成，数据量：{len(df_sina)}")
    else:
        print("未获取到realtime_index_sina数据")

    # 第三步：清理超过90个交易日的旧数据
    cleanup_old_kline_data()

    print(f"总计写入 {total_rows} 条记录，耗时 {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    main()