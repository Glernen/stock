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




def get_stock_basic_info():
    """查询股票基础信息并返回DataFrame"""
    sql = """
    SELECT
        `rss`.`code` AS `code`,
        `rss`.`code_int` AS `code_int`,
        `rss`.`name` AS `name`,
        `rsd`.`市场标识` AS `市场标识`,
        `rsd`.`所处行业` AS `东方财富网行业`,
        CONCAT(`rsd`.`市场标识`, '.', LPAD(`rss`.`code`, 6, '0')) AS `市场代码`,
        `rst`.`symbol` AS `symbol`,
        `rst`.`股票类型` AS `股票类型`,
        `hy1`.`name` AS `申万1级名`,
        `hy1`.`code` AS `申万1级代码`,
        `hy1`.`code_int` AS `申万1级数字代码`,
        `hy2`.`name` AS `申万2级名`,
        `hy2`.`code` AS `申万2级代码`,
        `hy2`.`code_int` AS `申万2级数字代码`,
        (CASE
            WHEN ((`rss`.`name` LIKE '*ST%')
                OR (`rss`.`name` LIKE 'ST%')
                OR (`rss`.`name` LIKE '%退%')) THEN 0
            WHEN (LOWER(SUBSTR(`rst`.`symbol`, 1, 2)) = 'bj') THEN 0
            WHEN (`rst`.`股票类型` = 'GP-A-KCB') THEN 0
            ELSE 1
        END) AS `参与指标计算`
    FROM
        ((((`realtime_stock_sina` `rss`
    JOIN `realtime_stock_df` `rsd` ON
        (((`rss`.`code_int` = `rsd`.`code_int`)
            AND (`rss`.`date_int` = `rsd`.`date_int`))))
    JOIN `realtime_stock_tx` `rst` ON
        (((`rss`.`code_int` = `rst`.`code_int`)
            AND (`rss`.`date_int` = `rst`.`date_int`))))
    LEFT JOIN `realtime_industry_tx` `hy1` ON
        (((`rss`.`code_int` = `hy1`.`lzg_code_int`)
            AND (`hy1`.`行业类型` = 'BK-HY-1')
                AND (`hy1`.`date_int` = `rss`.`date_int`))))
    LEFT JOIN `realtime_industry_tx` `hy2` ON
        (((`rss`.`code_int` = `hy2`.`lzg_code_int`)
            AND (`hy2`.`行业类型` = 'BK-HY-2')
                AND (`hy2`.`date_int` = `rss`.`date_int`))))
    WHERE
        `rss`.`date_int` = (SELECT MAX(`date_int`) FROM `realtime_stock_sina`);
        # `rss`.`date_int` = 20250715;
    """

    print("正在查询股票基础信息...")
    df = DBManager.query_sql(sql)
    print(f"查询完成，获取到 {len(df)} 条记录")
    print(df.head())
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
    unique_keys = ['code', 'code_int']

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



def main():
    start_time = time.time()

    # 获取股票基础信息
    basic_info_stock = get_stock_basic_info()

    if basic_info_stock.empty:
        print("未获取到股票基础信息数据，程序终止")
        return

    # 生成批量SQL
    sql_batches = sql_batch_generator(
        table_name='basic_info_stock',
        data=basic_info_stock,
        batch_size=6000  # 根据实际情况调整
    )

    # 执行批量插入
    execute_batch_sql(sql_batches)
    print(f"[Success]股票基础信息表数据写入完成，数据量：{len(basic_info_stock)}，耗时 {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    main()