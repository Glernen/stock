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




# numeric_cols = ["f2", "f3", "f4", "f5", "f6", "f7", "f8", "f10", "f15", "f16", "f17", "f18", "f22", "f11", "f24", "f25", "f9", "f115", "f114", "f23", "f112", "f113", "f61", "f48", "f37", "f49", "f57", "f40", "f41", "f45", "f46", "f38", "f39", "f20", "f21" ]
# date_cols = ["f26", "f221"]


########################################
#获取沪市A股+深市A股历史数据并写入数据库
def fetch_all_stock_hist(beg: str = None, end: str = None):
    """获取全量股票历史数据（支持日期区间）"""
    # 处理日期参数
    today = datetime.datetime.now().strftime("%Y%m%d")
    beg = beg or today
    end = end or today

    # 创建表
    for table_config in [
        tbs.CN_STOCK_HIST_DAILY_DATA,
        tbs.CN_STOCK_HIST_WEEKLY_DATA,
        tbs.CN_STOCK_HIST_MONTHLY_DATA
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code, market_id, name FROM cn_stock_info WHERE date = (SELECT MAX(date) FROM cn_stock_info WHERE code = '000001')", conn)
    conn.close()


    # 准备数据容器
    daily_data = pd.DataFrame()
    weekly_data = pd.DataFrame()
    monthly_data = pd.DataFrame()

    # 多线程获取数据
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for _, row in stock_df.iterrows():
            code = row['code']
            market_id = row['market_id']
            name = row['name']
            for period in ["daily", "weekly", "monthly"]:
                futures.append(
                    executor.submit(
                        fetch_single_hist,
                        code, market_id, period, name, "stock", beg, end
                    )
                )

        # 合并数据
        for future in tqdm(as_completed(futures), total=len(futures), desc="获取股票历史数据"):
            data = future.result()
            if data is None:
                continue
            df = data['df']
            if data['period'] == 'daily':
                daily_data = pd.concat([daily_data, df], ignore_index=True)
            elif data['period'] == 'weekly':
                weekly_data = pd.concat([weekly_data, df], ignore_index=True)
            elif data['period'] == 'monthly':
                monthly_data = pd.concat([monthly_data, df], ignore_index=True)

    # 同步表结构并写入数据
    def sync_and_write(table_name, data):
        conn = DBManager.get_new_connection()
        try:
            同步表结构(conn, table_name, data.columns)
        finally:
            if conn.is_connected():
                conn.close()
        sql_txt = sql语句生成器(table_name, data)
        if not execute_raw_sql(sql_txt):
            raise Exception(f"{table_name} 写入失败")

    sync_and_write(tbs.CN_STOCK_HIST_DAILY_DATA['name'], daily_data)
    sync_and_write(tbs.CN_STOCK_HIST_WEEKLY_DATA['name'], weekly_data)
    sync_and_write(tbs.CN_STOCK_HIST_MONTHLY_DATA['name'], monthly_data)
    
    print(f"[Success] 股票历史数据写入完成（日：{len(daily_data)}，周：{len(weekly_data)}，月：{len(monthly_data)}）")

########################################
#获取ETF基金历史数据并写入数据库
def fetch_all_etf_hist(beg: str = None, end: str = None):
    """获取全量股票历史数据（支持日期区间）"""
    # 处理日期参数
    today = datetime.datetime.now().strftime("%Y%m%d")
    beg = beg or today
    end = end or today

    # 创建表
    for table_config in [
        tbs.CN_ETF_HIST_DAILY_DATA,
        tbs.CN_ETF_HIST_WEEKLY_DATA,
        tbs.CN_ETF_HIST_MONTHLY_DATA
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code, market_id, name FROM cn_etf_info WHERE date = (SELECT MAX(date) FROM cn_etf_info WHERE code = '159001')", conn)
    conn.close()

    # 准备数据容器
    daily_data = pd.DataFrame()
    weekly_data = pd.DataFrame()
    monthly_data = pd.DataFrame()

    # 多线程获取数据
    with ThreadPoolExecutor() as executor:
        futures = []
        for _, row in stock_df.iterrows():
            code = row['code']
            market_id = row['market_id']
            name = row['name']
            for period in ["daily", "weekly", "monthly"]:
                futures.append(
                    executor.submit(
                        fetch_single_hist,
                        code, market_id, period, name, "stock", beg, end
                    )
                )

        # 合并数据
        for future in tqdm(as_completed(futures), total=len(futures), desc="获取股票历史数据"):
            data = future.result()
            if data is None:
                continue
            df = data['df']
            if data['period'] == 'daily':
                daily_data = pd.concat([daily_data, df], ignore_index=True)
            elif data['period'] == 'weekly':
                weekly_data = pd.concat([weekly_data, df], ignore_index=True)
            elif data['period'] == 'monthly':
                monthly_data = pd.concat([monthly_data, df], ignore_index=True)

    # 同步表结构并写入数据
    def sync_and_write(table_name, data):
        conn = DBManager.get_new_connection()
        try:
            同步表结构(conn, table_name, data.columns)
        finally:
            if conn.is_connected():
                conn.close()
        sql_txt = sql语句生成器(table_name, data)
        if not execute_raw_sql(sql_txt):
            raise Exception(f"{table_name} 写入失败")

    sync_and_write(tbs.CN_ETF_HIST_DAILY_DATA['name'], daily_data)
    sync_and_write(tbs.CN_ETF_HIST_WEEKLY_DATA['name'], weekly_data)
    sync_and_write(tbs.CN_ETF_HIST_MONTHLY_DATA['name'], monthly_data)
    
    print(f"[Success] 基金历史数据写入完成（日：{len(daily_data)}，周：{len(weekly_data)}，月：{len(monthly_data)}）")

########################################
#获取指数历史数据并写入数据库
def fetch_all_index_hist(beg: str = None, end: str = None):
    """获取全量股票历史数据（支持日期区间）"""
    # 处理日期参数
    today = datetime.datetime.now().strftime("%Y%m%d")
    beg = beg or today
    end = end or today

    # 创建表
    for table_config in [
        tbs.CN_INDEX_HIST_DAILY_DATA,
        tbs.CN_INDEX_HIST_WEEKLY_DATA,
        tbs.CN_INDEX_HIST_MONTHLY_DATA
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code, market_id, name FROM cn_index_info WHERE date = (SELECT MAX(date) FROM cn_index_info WHERE code = '000001')", conn)
    conn.close()

    # 准备数据容器
    daily_data = pd.DataFrame()
    weekly_data = pd.DataFrame()
    monthly_data = pd.DataFrame()

    # 多线程获取数据
    with ThreadPoolExecutor() as executor:
        futures = []
        for _, row in stock_df.iterrows():
            code = row['code']
            market_id = row['market_id']
            name = row['name']
            for period in ["daily", "weekly", "monthly"]:
                futures.append(
                    executor.submit(
                        fetch_single_hist,
                        code, market_id, period, name, "stock", beg, end
                    )
                )

        # 合并数据
        for future in tqdm(as_completed(futures), total=len(futures), desc="获取股票历史数据"):
            data = future.result()
            if data is None:
                continue
            df = data['df']
            if data['period'] == 'daily':
                daily_data = pd.concat([daily_data, df], ignore_index=True)
            elif data['period'] == 'weekly':
                weekly_data = pd.concat([weekly_data, df], ignore_index=True)
            elif data['period'] == 'monthly':
                monthly_data = pd.concat([monthly_data, df], ignore_index=True)

    # 同步表结构并写入数据
    def sync_and_write(table_name, data):
        conn = DBManager.get_new_connection()
        try:
            同步表结构(conn, table_name, data.columns)
        finally:
            if conn.is_connected():
                conn.close()
        sql_txt = sql语句生成器(table_name, data)
        if not execute_raw_sql(sql_txt):
            raise Exception(f"{table_name} 写入失败")

    sync_and_write(tbs.CN_INDEX_HIST_DAILY_DATA['name'], daily_data)
    sync_and_write(tbs.CN_INDEX_HIST_WEEKLY_DATA['name'], weekly_data)
    sync_and_write(tbs.CN_INDEX_HIST_MONTHLY_DATA['name'], monthly_data)
    
    print(f"[Success] 指数历史数据写入完成（日：{len(daily_data)}，周：{len(weekly_data)}，月：{len(monthly_data)}）")


def fetch_single_hist(code: str, market_id: str, period: str, name: str, data_type: str, beg: str, end: str):
    """通用函数：获取单个代码的历史数据（股票/ETF/指数）"""
    try:
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

        # 根据数据类型选择配置
        table_config_map = {
            "stock": tbs.CN_STOCK_HIST_DAILY_DATA,
            "etf": tbs.CN_ETF_HIST_DAILY_DATA,
            "index": tbs.CN_INDEX_HIST_DAILY_DATA
        }
        table_config = table_config_map[data_type]

        # 请求参数（ut 统一使用一个值，如原股票/ETF的ut）
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",  # 统一使用股票/ETF的ut
            "klt": {'daily':101, 'weekly':102, 'monthly':103}[period],
            "fqt": 0,
            "secid": f"{market_id}.{code}",
            # "beg": "20200101",
            # "end": datetime.datetime.now().strftime("%Y%m%d"),
            "beg": beg,  # 使用传入的beg
            "end": end,  # 使用传入的end
            "_": int(time.time()*1000)
        }

        # 发送请求与数据处理（与原逻辑一致）
        r = requests.get(url, params=params, timeout=10)
        data_json = r.json()
        if not data_json.get("data"):
            return None

        # 数据处理与字段映射
        df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        en_name = {
            table_config['columns'][k]['map']: table_config['columns'][k]['en']
            for k in table_config['columns']
            if 'map' in table_config['columns'][k]
        }
        
        # 格式转换与字段添加
        numeric_cols = [1,2,3,4,5,6,7,8,9,10]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df.rename(columns=en_name, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df['code'] = code
        df['code_int'] = code
        df['period'] = period
        df['name'] = name
        
        return {'period': period, 'df': df, 'df_columns': df.columns}
    except Exception as e:
        print(f"获取{code} {period}数据失败: {str(e)}")
        return None


"""
定义公共函数：
create_table_if_not_exists(table_name)：检查数据表是否存在，如果不存在，则创建数据库并添加索引
同步表结构(conn, table_name, data_columns)： 动态检查并自动添加表中缺失的字段
sql语句生成器(table_name,data)：带入参数数据表名和数据，生成数据插入语句
execute_raw_sql(sql,params)：执行插入数据表
convert_date_format(input_date: str)：将 yyyy-mm-dd 格式转换为 yyyymmdd 格式
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


def create_table_if_not_exists(table_name):
    # 创建表（不含索引）
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `date` DATE,
            `code` VARCHAR(6),
            `code_int` INT,
            `name` VARCHAR(20)
        );
    """
    DBManager.execute_sql(create_table_sql)
    
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

    # 添加索引
    create_index("idx_date_code", ["date", "code"], is_unique=True)
    create_index("idx_code_int", ["code_int"])
    create_index("idx_date", ["date"])


def 同步表结构(conn, table_name, data_columns):
    """动态添加缺失字段，附带调试日志和错误处理"""
    try:
        cursor = conn.cursor(buffered=True)
        
        # 调试：打印基本信息
        # print(f"\n[DEBUG] 开始同步表结构：{table_name}")
        # print(f"[DEBUG] 数据列要求字段：{data_columns}")

        # 获取现有字段
        cursor.execute(f"DESCRIBE `{table_name}`;")
        existing_columns = [row[0] for row in cursor.fetchall()]
        # print(f"[DEBUG] 数据库现有字段：{existing_columns}")

        # 获取配置表字段
        table_config = tbs.TABLE_REGISTRY.get(table_name, {})
        all_required_columns = list(table_config.get('columns', {}).keys())
        # print(f"[DEBUG] 配置表要求字段：{all_required_columns}")

        # 遍历处理字段
        for col in data_columns:
            try:
                # print(f"\n[DEBUG] 正在检查字段：{col}")
                
                if col not in existing_columns:
                    if col in all_required_columns:
                        # 从配置获取字段类型
                        col_info = table_config['columns'][col]
                        sql_type = tbs._get_sql_type(col_info['type'])
                        
                        # 执行添加字段
                        alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {sql_type};"
                        print(f"[EXECUTE] 执行SQL：{alter_sql}")
                        
                        cursor.execute(alter_sql)
                        # print(f"[SUCCESS] 字段 {col} 添加成功")
                    else:
                        print(f"[WARNING] 字段 {col} 不在配置表中，已跳过")
                else:
                    pass
                    # print(f"[INFO] 字段 {col} 已存在，无需添加")
            except Exception as col_error:
                print(f"[ERROR] 处理字段 {col} 时发生错误：{str(col_error)}")
                conn.rollback()  # 回滚当前字段操作

        conn.commit()
        print(f"[SUCCESS] 表 {table_name} 结构同步完成")

    except Exception as main_error:
        print(f"[CRITICAL] 同步表结构主流程失败：{str(main_error)}")
        conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            print("[INFO] 数据库游标已关闭")


def sql语句生成器(table_name, data):
    # # 准备模板
    # sql_template = """INSERT INTO `{table_name}` ({columns}) VALUES {values} ON DUPLICATE KEY UPDATE {update_clause};"""
    # # 预处理列名和更新子句
    # columns = ', '.join([f"`{col}`" for col in data.columns])
    # update_clause = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in data.columns if col not in ['date', 'code']])
    # 准备模板
    # 
    # 
    # 预处理数据（如添加code）
    if 'code' in data.columns and 'code' not in data.columns:
        data.insert(0, 'code', data['code'].astype(str).str.zfill(6))

    sql_template = """INSERT INTO `{table_name}` ({columns}) 
        VALUES {values} 
        ON DUPLICATE KEY UPDATE {update_clause};"""

    # 预处理列名
    columns = ', '.join([f"`{col}`" for col in data.columns])

    # 指定唯一键列（这些字段用于比对数据库中的记录）
    unique_keys = ['date', 'code'] 

    # 更新子句：更新所有非唯一键的列
    update_clause = ', '.join(
        [f"`{col}`=VALUES(`{col}`)"
         for col in data.columns
         if col not in unique_keys]
    )
    # 批量处理值
    value_rows = []
    for row in data.itertuples(index=False):
        values = []
        for item in row:
            if pd.isna(item) or item in ['-', '']:
                values.append("NULL")
            elif isinstance(item, (datetime.date, datetime.datetime)):
                values.append(f"'{item.strftime('%Y-%m-%d')}'")
            elif isinstance(item, (int, float, bool)):
                values.append(str(item))
            else:
                # 使用三重引号避免嵌套冲突
                cleaned_item = str(item).replace("'", "''")
                values.append(f"'{cleaned_item}'")
        value_rows.append(f"({', '.join(values)})")

    # 批量生成SQL
    sql_statements = [
        sql_template.format(
            table_name=table_name,
            columns=columns,
            values=values,
            update_clause=update_clause
        )
        for values in value_rows
    ]

    # 连接所有语句
    sql_txt = "\n".join(sql_statements)
    return sql_txt


def execute_raw_sql(sql, params=None, max_query_size=1024*1024, batch_size=5000):
    """改进后的SQL执行函数，解决Commands out of sync问题"""
    connection = DBManager.get_new_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor(buffered=True)  # 使用缓冲游标
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        for i in range(0, len(statements), batch_size):
            batch = statements[i:i+batch_size]
            for statement in batch:
                try:
                    cursor.execute(statement)
                    cursor.fetchall()  # 显式消费结果集
                except Error as e:
                    print(f"执行失败: {statement[:50]}... | 错误: {e}")
                    connection.rollback()
                    return False
            connection.commit()  # 每批提交一次
        return True
    except Error as e:
        print(f"数据库错误: {e}")
        connection.rollback()
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def convert_date_format(input_date: str) -> str:
    """将 yyyy-mm-dd 格式转换为 yyyymmdd 格式"""
    try:
        # 尝试解析 yyyy-mm-dd 格式
        date_obj = datetime.datetime.strptime(input_date, "%Y-%m-%d")
        return date_obj.strftime("%Y%m%d")
    except ValueError:
        # 如果已经是 yyyymmdd 格式则直接返回
        try:
            datetime.datetime.strptime(input_date, "%Y%m%d")
            return input_date
        except ValueError:
            raise ValueError(f"无效的日期格式：{input_date}，请输入 yyyy-mm-dd 或 yyyymmdd 格式")




# 新增函数：交易日判断逻辑
def get_shanghai_calendar():
    """获取上证交易所日历"""
    return mcal.get_calendar('SSE')

def is_trading_day(date: datetime.datetime) -> bool:
    """判断单个日期是否为交易日"""
    calendar = get_shanghai_calendar()
    date_str = date.strftime('%Y-%m-%d')
    schedule = calendar.schedule(start_date=date_str, end_date=date_str)
    return not schedule.empty

def is_within_trading_hours(now=None):
    """判断当前时间是否在交易时段内（收盘后视为有效）"""
    tz = pytz.timezone('Asia/Shanghai')
    now = now or datetime.datetime.now(tz)
    
    # 判断是否为交易日
    if not is_trading_day(now):
        return False
    
    # 判断是否在收盘时间后（15:00后视为收盘）
    market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)
    return now >= market_close

def is_valid_trading_period(start_date: str, end_date: str) -> bool:
    """验证日期区间是否包含有效交易日"""
    calendar = get_shanghai_calendar()
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    return not schedule.empty

# 修改后的main函数
def main():
    sh_tz = pytz.timezone('Asia/Shanghai')
    
    # 场景1: 无参数（自动判断当天）
    if len(sys.argv) == 1:
        now = datetime.datetime.now(sh_tz)
        if not is_within_trading_hours(now):
            print(f"[{now.strftime('%Y-%m-%d %H:%M')}] 非交易时段，终止执行")
            return
        today = now.strftime("%Y%m%d")
        fetch_all_stock_hist(today, today)
        fetch_all_etf_hist(today, today)
        fetch_all_index_hist(today, today)
    
    # 场景2: 单日期模式
    elif len(sys.argv) == 2:
        date_str = sys.argv[1]
        try:
            clean_date = convert_date_format(date_str)
            dt = sh_tz.localize(datetime.datetime.strptime(clean_date, "%Y%m%d"))
            
            if not is_trading_day(dt):
                print(f"[{date_str}] 非交易日，终止执行")
                return
                
            fetch_all_stock_hist(clean_date, clean_date)
            fetch_all_etf_hist(clean_date, clean_date)
            fetch_all_index_hist(clean_date, clean_date)
            
        except ValueError as e:
            print(f"日期格式错误：{e}")
    
    # 场景3: 日期区间模式
    elif len(sys.argv) == 3:
        try:
            beg_str = convert_date_format(sys.argv[1])
            end_str = convert_date_format(sys.argv[2])
            
            if not is_valid_trading_period(beg_str, end_str):
                print(f"[{beg_str}-{end_str}] 区间无交易日，终止执行")
                return
                
            fetch_all_stock_hist(beg_str, end_str)
            fetch_all_etf_hist(beg_str, end_str)
            fetch_all_index_hist(beg_str, end_str)
            
        except ValueError as e:
            print(f"日期格式错误：{e}")
    
    else:
        print("参数错误！支持以下调用方式：")
        print("1. 无参数       -> 自动判断当天交易时段")
        print("2. 单日期       -> python script.py 2023-01-01")
        print("3. 日期区间     -> python script.py 2023-01-01 2023-01-05")


# main函数入口
if __name__ == '__main__':
    main()

