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
import pytz
from mysql.connector import Error
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset


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
    def table_exists(table_name):
        """检查数据表是否存在"""
        connection = DBManager.get_new_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                result = cursor.fetchone()
                return result is not None
            except Error as e:
                print(f"Error while checking table existence: {e}")
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()
        return False


def create_temp_table(source_table: str, temp_table: str):
    """创建临时表并填充最近五个交易日的数据"""
    # 删除旧临时表
    DBManager.execute_sql(f"DROP TABLE IF EXISTS `{temp_table}`")

    # 获取最近五个交易日
    conn = DBManager.get_new_connection()
    try:
        cursor = conn.cursor()
        # 获取最近的五个交易日日期
        cursor.execute(f"""
            SELECT DISTINCT date_int 
            FROM `{source_table}` 
            ORDER BY date_int DESC 
            LIMIT 5
        """)
        recent_dates = [str(row[0]) for row in cursor.fetchall()]

        if not recent_dates:
            print(f"源表 {source_table} 中没有找到交易日数据")
            return

        date_condition = ", ".join(recent_dates)

        # 创建临时表并填充数据
        create_sql = f"""
            CREATE TABLE `{temp_table}` AS
            SELECT * 
            FROM `{source_table}`
            WHERE date_int IN ({date_condition})
        """
        cursor.execute(create_sql)
        conn.commit()
        print(f"成功创建临时表 {temp_table}，包含 {len(recent_dates)} 个交易日的数据")

        # 为临时表创建索引
        index_sql = f"""
            CREATE INDEX idx_{temp_table}_date_int ON `{temp_table}` (date_int);
            CREATE INDEX idx_{temp_table}_code_int ON `{temp_table}` (code_int);
        """
        for stmt in index_sql.split(";"):
            if stmt.strip():
                cursor.execute(stmt)
        conn.commit()

    except Error as e:
        print(f"创建临时表 {temp_table} 失败: {e}")
        conn.rollback()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def process_3day_data(source_table: str, target_table: str, sample_code: int):
    """
    处理三日指标数据并写入目标表
    :param source_table: 源数据表名
    :param target_table: 目标表名
    :param sample_code: 用于获取最新日期的示例代码
    """
    # 第一步：创建目标表
    create_3day_table(target_table)

    # 第二步：执行分析查询
    # 根据源表类型确定关联表
    join_clause = ""
    if "stock" in source_table:
        join_clause = f"""
        JOIN basic_info_stock s ON s.code_int = t.code_int
        JOIN kline_stock hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int
        """
    elif "etf" in source_table:
        join_clause = "JOIN kline_etf hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int"
    elif "index" in source_table:
        join_clause = "JOIN kline_index hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int"
    elif "industry" in source_table:
        join_clause = "JOIN kline_industry hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int"

    # 根据源表类型选择字段
    industry_field = "s.`东方财富网行业` AS industry," if "stock" in source_table else "NULL AS industry,"

    # 第二步：执行分析查询
    query = f"""
    WITH LatestDate AS (
        SELECT MAX(date) AS last_date
        FROM {source_table}
        WHERE code_int = {sample_code}
    ),
    3day AS (
        SELECT
            t.code_int,
            t.date_int,
            t.code,
            t.date,
            t.name,
            hist.close,
            t.kdjk,
            LAG(t.kdjk, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS kdjk_day1,
            LAG(t.kdjk, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS kdjk_day2,
            t.kdjd,
            LAG(t.kdjd, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS kdjd_day1,
            LAG(t.kdjd, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS kdjd_day2,
            t.kdjj,
            LAG(t.kdjj, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS kdjj_day1,
            LAG(t.kdjj, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS kdjj_day2,
            t.wr_6,
            LAG(t.wr_6, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS wr_6_day1,
            LAG(t.wr_6, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS wr_6_day2,
            t.wr_10,
            LAG(t.wr_10, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS wr_10_day1,
            LAG(t.wr_10, 2) OVER (PARTition BY t.code_int ORDER BY t.date_int) AS wr_10_day2,
            t.rsi_6,
            LAG(t.rsi_6, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS rsi_6_day1,
            LAG(t.rsi_6, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS rsi_6_day2,
            t.rsi_12,
            LAG(t.rsi_12, 1) OVER (PARTition BY t.code_int ORDER BY t.date_int) AS rsi_12_day1,
            LAG(t.rsi_12, 2) OVER (PARTition BY t.code_int ORDER BY t.date_int) AS rsi_12_day2,
            t.cci,
            LAG(t.cci, 1) OVER (PARTition BY t.code_int ORDER BY t.date_int) AS cci_day1,
            LAG(t.cci, 2) OVER (PARTition BY t.code_int ORDER BY t.date_int) AS cci_day2,
            {industry_field}
            hist.turnoverrate AS turnover
        FROM {source_table} t
        {join_clause}
        WHERE t.date_int >= (SELECT MIN(date_int) FROM {source_table})
    )
    SELECT * FROM 3day
    WHERE kdjk_day2 IS NOT NULL
      AND kdjd_day2 IS NOT NULL
      AND kdjj_day2 IS NOT NULL
      AND wr_6_day2 IS NOT NULL
      AND wr_10_day2 IS NOT NULL
      AND rsi_6_day2 IS NOT NULL
      AND rsi_12_day2 IS NOT NULL
      AND cci_day2 IS NOT NULL;
    """

    # 执行查询并获取数据
    conn = DBManager.get_new_connection()
    try:
        df = pd.read_sql(query, conn)
        print(f"从 {source_table} 获取到 {len(df)} 条三日指标数据")

        if not df.empty:
            # 第三步：写入目标表
            sql_batches = sql语句生成器(target_table, df)
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = [executor.submit(DBManager.execute_sql, sql) for sql in sql_batches]
                for future in tqdm(as_completed(futures), total=len(futures), desc="写入进度"):
                    future.result()
            print(f"{target_table} 数据更新完成，新增 {len(df)} 条记录")
        else:
            print(f"未从 {source_table} 获取到有效数据")
    except Error as e:
        print(f"处理 {source_table} 时发生数据库错误: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()


def create_3day_table(table_name: str):
    """创建三日指标表结构"""
    columns = [
        ('id', 'INT AUTO_INCREMENT PRIMARY KEY'),
        ('date', 'DATE'), ('date_int', 'INT'),
        ('code_int', 'INT'), ('code', 'VARCHAR(6)'),
        ('name', 'VARCHAR(20)'), ('close', 'FLOAT'),
        ('turnover', 'FLOAT'), ('industry', 'VARCHAR(20)'),
        ('kdjk', 'FLOAT'), ('kdjk_day1', 'FLOAT'), ('kdjk_day2', 'FLOAT'),
        ('kdjd', 'FLOAT'), ('kdjd_day1', 'FLOAT'), ('kdjd_day2', 'FLOAT'),
        ('kdjj', 'FLOAT'), ('kdjj_day1', 'FLOAT'), ('kdjj_day2', 'FLOAT'),
        ('wr_6', 'FLOAT'), ('wr_6_day1', 'FLOAT'), ('wr_6_day2', 'FLOAT'),
        ('wr_10', 'FLOAT'), ('wr_10_day1', 'FLOAT'), ('wr_10_day2', 'FLOAT'),
        ('rsi_6', 'FLOAT'), ('rsi_6_day1', 'FLOAT'), ('rsi_6_day2', 'FLOAT'),
        ('rsi_12', 'FLOAT'), ('rsi_12_day1', 'FLOAT'), ('rsi_12_day2', 'FLOAT'),
        ('cci', 'FLOAT'), ('cci_day1', 'FLOAT'), ('cci_day2', 'FLOAT')
    ]

    # 创建表
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    create_sql += ",\n".join([f"`{col[0]}` {col[1]}" for col in columns])
    create_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

    DBManager.execute_sql(create_sql)

    if not DBManager.table_exists(table_name):
        # 创建索引
        index_sql = f"""
        CREATE UNIQUE INDEX idx_{table_name}_date_code_int 
        ON {table_name} (date_int, code_int);

        CREATE INDEX idx_{table_name}_code_int 
        ON {table_name} (code_int);

        CREATE INDEX idx_{table_name}_date_int 
        ON {table_name} (date_int);
        """
        for statement in index_sql.split(';'):
            if statement.strip():
                DBManager.execute_sql(statement)


def sql语句生成器(table_name, data, batch_size=5000):
    # 预处理code_int字段
    if 'code' in data.columns and 'code_int' not in data.columns:
        data.insert(0, 'code_int', data['code'].astype(int))

    # 增加数据排序
    data = data.sort_values(by=['code_int', 'date_int'])

    # SQL模板（批量版本）
    sql_template = """INSERT INTO `{table_name}` ({columns}) 
        VALUES {values}
        ON DUPLICATE KEY UPDATE {update_clause};"""

    # 定义字段和更新子句
    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'code_int']
    update_clause = ', '.join(
        [f"`{col}`=VALUES(`{col}`)"
         for col in data.columns if col not in unique_keys]
    )

    # 分批次处理数据
    batches = []
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        value_rows = []

        for row in batch.itertuples(index=False):
            values = []
            for item in row:
                # 处理空值和特殊字符
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

        # 合并为单个VALUES子句
        values_str = ',\n'.join(value_rows)
        batches.append(
            sql_template.format(
                table_name=table_name,
                columns=columns,
                values=values_str,
                update_clause=update_clause
            )
        )

    return batches


def main():
    # 时区设置
    tz = pytz.timezone('Asia/Shanghai')
    pd.Timestamp.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    start_time = time.time()

    # 定义源表和目标表映射
    table_mappings = [
        ("cn_stock_indicators", "temp_stock_indicators", "stock_3day_indicators", 1),
        ("cn_etf_indicators", "temp_etf_indicators", "etf_3day_indicators", 159001),
        ("cn_index_indicators", "temp_index_indicators", "index_3day_indicators", 1),
        ("cn_industry_indicators", "temp_industry_indicators", "industry_3day_indicators", 447)
    ]

    try:
        # 第一步：创建临时表
        for source_table, temp_table, _, _ in table_mappings:
            print(f"⏳ 正在创建临时表 {temp_table}...")
            create_temp_table(source_table, temp_table)

        # 第二步：并行处理三类数据
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = []
            for source_table, temp_table, target_table, sample_code in table_mappings:
                futures.append(
                    executor.submit(process_3day_data,
                                    temp_table,
                                    target_table,
                                    sample_code)
                )

            # 等待所有任务完成
            for future in tqdm(as_completed(futures), total=len(futures), desc="处理进度"):
                try:
                    future.result()
                except Exception as e:
                    print(f"处理过程中发生错误: {e}")

        # 第三步：清理临时表
        for _, temp_table, _, _ in table_mappings:
            print(f"🧹 清理临时表 {temp_table}...")
            DBManager.execute_sql(f"DROP TABLE IF EXISTS `{temp_table}`")

    finally:
        print(f"\n🕒 三日指标，总耗时: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    main()