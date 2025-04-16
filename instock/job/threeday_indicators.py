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
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def create_table_if_not_exists(table_name):
    # 创建表（不含索引）
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `date` DATE,
            `date_int` INT,
            `code_int` INT,
            `code` VARCHAR(6),
            `name` VARCHAR(20)
        );
    """
    DBManager.execute_sql(create_table_sql)

    # 检查并添加id列（如果不存在）
    conn = DBManager.get_new_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # 检查id列是否存在
            check_sql = f"""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{table_name}'
                  AND COLUMN_NAME = 'id';
            """
            cursor.execute(check_sql)
            count = cursor.fetchone()[0]
            if count == 0:
                # 添加id列
                alter_sql = f"""
                    ALTER TABLE `{table_name}`
                    ADD COLUMN `id` INT AUTO_INCREMENT PRIMARY KEY FIRST;
                """
                cursor.execute(alter_sql)
                conn.commit()
                print(f"表 {table_name} 成功添加 id 列")
        except Error as e:
            print(f"检查或添加 id 列失败: {e}")
            conn.rollback()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    # 创建索引（修复 Unread result found 问题）
    def create_index(index_name, columns, is_unique=False):
        conn = DBManager.get_new_connection()
        try:
            cursor = conn.cursor()
            # 检查索引是否存在
            check_sql = f"""
                SELECT COUNT(*)
                FROM information_schema.STATISTICS
                WHERE table_name = '{table_name}'
                  AND index_name = '{index_name}';
            """
            cursor.execute(check_sql)
            result = cursor.fetchall()  # 强制读取结果
            if result[0][0] == 0:
                index_type = "UNIQUE" if is_unique else ""
                create_sql = f"""
                    CREATE {index_type} INDEX `{index_name}`
                    ON `{table_name}` ({', '.join(columns)});
                """
                cursor.execute(create_sql)
                conn.commit()
        except Error as e:
            print(f"创建索引 {index_name} 失败: {e}")
            conn.rollback()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    # if not DBManager.table_exists(table_name):
    #     # 只有当表不存在时才创建索引
    #     create_index("idx_date_code_int", ["date_int", "code_int"], is_unique=True)
    #     create_index("idx_code_int", ["code_int"])
    #     create_index("idx_date_int", ["date_int"])


def sql语句生成器(table_name, data, batch_size=500):

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
        batch = data.iloc[i:i+batch_size]
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
        JOIN cn_stock_info s ON s.code_int = t.code_int
        JOIN cn_stock_hist_daily hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int
        """
    elif "etf" in source_table:
        join_clause = "JOIN cn_etf_hist_daily hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int"
    elif "index" in source_table:
        join_clause = "JOIN cn_index_hist_daily hist ON hist.code_int = t.code_int AND hist.date_int = t.date_int"

    # 根据源表类型选择字段
    industry_field = "s.industry," if "stock" in source_table else "NULL AS industry,"
    
    # 第二步：执行分析查询
    query = f"""
    WITH LatestDate AS (
        SELECT MAX(date_int) AS last_date 
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
            t.close,
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
            LAG(t.wr_10, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS wr_10_day2,
            t.rsi_6,
            LAG(t.rsi_6, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS rsi_6_day1,
            LAG(t.rsi_6, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS rsi_6_day2,
            t.rsi_12,
            LAG(t.rsi_12, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS rsi_12_day1,
            LAG(t.rsi_12, 2) OVER (PARTition BY t.code_int ORDER BY t.date_int) AS rsi_12_day2,
            t.cci,
            LAG(t.cci, 1) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS cci_day1,
            LAG(t.cci, 2) OVER (PARTITION BY t.code_int ORDER BY t.date_int) AS cci_day2,
            {industry_field}
            hist.turnover
        FROM {source_table} t
        {join_clause}
        WHERE t.date_int BETWEEN (SELECT last_date - 20 FROM LatestDate) AND (SELECT last_date FROM LatestDate)
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
        ('name', 'VARCHAR(20)'),('close', 'FLOAT'), 
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


def main():
    # 并行处理三类数据
    with ThreadPoolExecutor(max_workers=3) as executor:
        # 股票三日指标
        executor.submit(process_3day_data, 
                       'cn_stock_indicators', 
                       'stock_3day_indicators',
                       1)  # 示例代码000001
        
        # ETF三日指标
        executor.submit(process_3day_data,
                       'cn_etf_indicators',
                       'etf_3day_indicators',
                       159001)  # 示例代码159001
        
        # 指数三日指标
        executor.submit(process_3day_data,
                       'cn_index_indicators',
                       'index_3day_indicators',
                       1)  # 示例代码000001

if __name__ == "__main__":
    # 时区设置
    tz = pytz.timezone('Asia/Shanghai')
    pd.Timestamp.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    
    start_time = time.time()
    main()
    print(f"总耗时：{time.time()-start_time:.2f}秒")


# /*股票三日指标数据分析,储到stock_3day_indicators,*/
# -- 定义 CTE 获取最新日期
# WITH LatestDate AS (
#     SELECT MAX(date_int) AS last_date 
#     FROM cn_stock_indicators 
#     WHERE code_int = 1
# ),
# 3day AS (
#     SELECT
#         id,
#         code_int,
#         date_int,
#         code,
#         date,
#         name,
#         kdjk,
#         -- 获取前 1 日的 kdjk 值（按股票代码分组，按日期排序）
#         LAG(`kdjk`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjk_day1,
#         -- 获取前 2 日的 kdjk 值（按股票代码分组，按日期排序）
#         LAG(`kdjk`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjk_day2,
#         kdjd,
#         -- 获取前 1 日的 kdjd 值（按股票代码分组，按日期排序）
#         LAG(`kdjd`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjd_day1,
#         -- 获取前 2 日的 kdjd 值（按股票代码分组，按日期排序）
#         LAG(`kdjd`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjd_day2,
#         kdjj,
#         -- 获取前 1 日的 kdjj 值（按股票代码分组，按日期排序）
#         LAG(`kdjj`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjj_day1,
#         -- 获取前 2 日的 kdjj 值（按股票代码分组，按日期排序）
#         LAG(`kdjj`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjj_day2,
#         wr_6,
#         -- 获取前 1 日的 wr_6 值（按股票代码分组，按日期排序）
#         LAG(`wr_6`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS wr_6_day1,
#         -- 获取前 2 日的 wr_6 值（按股票代码分组，按日期排序）
#         LAG(`wr_6`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS wr_6_day2,
#         cci,
#         -- 获取前 1 日的 cci 值（按股票代码分组，按日期排序）
#         LAG(`cci`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS cci_day1,
#         -- 获取前 2 日的 cci 值（按股票代码分组，按日期排序）
#         LAG(`cci`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS cci_day2
#     FROM
#         cn_stock_indicators 
#     -- 筛选日期范围
#     WHERE
#         date_int BETWEEN (SELECT last_date - 10 FROM LatestDate) AND (SELECT last_date FROM LatestDate)
# )
# SELECT *
# FROM 3day
# WHERE
#     -- 排除字段中有 NULL 的行
#     kdjk_day2 IS NOT NULL
#     AND kdjd_day2 IS NOT NULL
#     AND kdjj_day2 IS NOT NULL
#     AND wr_6_day2 IS NOT NULL
#     AND cci_day2 IS NOT NULL;
    



# /*ETF三日指标数据分析，存储到etf_3day_indicators*/
# -- 定义 CTE 获取最新日期
# WITH LatestDate AS (
#     SELECT MAX(date_int) AS last_date 
#     FROM cn_etf_indicators 
#     WHERE code_int = 159001
# ),
# 3day AS (
#     SELECT
#         id,
#         code_int,
#         date_int,
#         code,
#         date,
#         name,
#         kdjk,
#         -- 获取前 1 日的 kdjk 值（按股票代码分组，按日期排序）
#         LAG(`kdjk`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjk_day1,
#         -- 获取前 2 日的 kdjk 值（按股票代码分组，按日期排序）
#         LAG(`kdjk`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjk_day2,
#         kdjd,
#         -- 获取前 1 日的 kdjd 值（按股票代码分组，按日期排序）
#         LAG(`kdjd`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjd_day1,
#         -- 获取前 2 日的 kdjd 值（按股票代码分组，按日期排序）
#         LAG(`kdjd`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjd_day2,
#         kdjj,
#         -- 获取前 1 日的 kdjj 值（按股票代码分组，按日期排序）
#         LAG(`kdjj`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjj_day1,
#         -- 获取前 2 日的 kdjj 值（按股票代码分组，按日期排序）
#         LAG(`kdjj`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjj_day2,
#         wr_6,
#         -- 获取前 1 日的 wr_6 值（按股票代码分组，按日期排序）
#         LAG(`wr_6`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS wr_6_day1,
#         -- 获取前 2 日的 wr_6 值（按股票代码分组，按日期排序）
#         LAG(`wr_6`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS wr_6_day2,
#         cci,
#         -- 获取前 1 日的 cci 值（按股票代码分组，按日期排序）
#         LAG(`cci`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS cci_day1,
#         -- 获取前 2 日的 cci 值（按股票代码分组，按日期排序）
#         LAG(`cci`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS cci_day2
#     FROM
#         cn_etf_indicators 
#     -- 筛选日期范围
#     WHERE
#         date_int BETWEEN (SELECT last_date - 10 FROM LatestDate) AND (SELECT last_date FROM LatestDate)
# )
# SELECT *
# FROM 3day
# WHERE
#     -- 排除字段中有 NULL 的行
#     kdjk_day2 IS NOT NULL
#     AND kdjd_day2 IS NOT NULL
#     AND kdjj_day2 IS NOT NULL
#     AND wr_6_day2 IS NOT NULL
#     AND cci_day2 IS NOT NULL;






# /*指数三日指标数据分析，存储到index_3day_indicators*/
# -- 定义 CTE 获取最新日期
# WITH LatestDate AS (
#     SELECT MAX(date_int) AS last_date 
#     FROM cn_index_indicators 
#     WHERE code_int = 1
# ),
# 3day AS (
#     SELECT
#         id,
#         code_int,
#         date_int,
#         code,
#         date,
#         name,
#         kdjk,
#         -- 获取前 1 日的 kdjk 值（按股票代码分组，按日期排序）
#         LAG(`kdjk`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjk_day1,
#         -- 获取前 2 日的 kdjk 值（按股票代码分组，按日期排序）
#         LAG(`kdjk`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjk_day2,
#         kdjd,
#         -- 获取前 1 日的 kdjd 值（按股票代码分组，按日期排序）
#         LAG(`kdjd`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjd_day1,
#         -- 获取前 2 日的 kdjd 值（按股票代码分组，按日期排序）
#         LAG(`kdjd`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjd_day2,
#         kdjj,
#         -- 获取前 1 日的 kdjj 值（按股票代码分组，按日期排序）
#         LAG(`kdjj`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjj_day1,
#         -- 获取前 2 日的 kdjj 值（按股票代码分组，按日期排序）
#         LAG(`kdjj`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS kdjj_day2,
#         wr_6,
#         -- 获取前 1 日的 wr_6 值（按股票代码分组，按日期排序）
#         LAG(`wr_6`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS wr_6_day1,
#         -- 获取前 2 日的 wr_6 值（按股票代码分组，按日期排序）
#         LAG(`wr_6`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS wr_6_day2,
#         cci,
#         -- 获取前 1 日的 cci 值（按股票代码分组，按日期排序）
#         LAG(`cci`, 1) OVER (PARTITION BY code_int ORDER BY date_int) AS cci_day1,
#         -- 获取前 2 日的 cci 值（按股票代码分组，按日期排序）
#         LAG(`cci`, 2) OVER (PARTITION BY code_int ORDER BY date_int) AS cci_day2
#     FROM
#         cn_index_indicators 
#     -- 筛选日期范围
#     WHERE
#         date_int BETWEEN (SELECT last_date - 10 FROM LatestDate) AND (SELECT last_date FROM LatestDate)
# )
# SELECT *
# FROM 3day
# WHERE
#     -- 排除字段中有 NULL 的行
#     kdjk_day2 IS NOT NULL
#     AND kdjd_day2 IS NOT NULL
#     AND kdjj_day2 IS NOT NULL
#     AND wr_6_day2 IS NOT NULL
#     AND cci_day2 IS NOT NULL;


