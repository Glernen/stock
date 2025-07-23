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
import instock.lib.database as mdb
import pandas_market_calendars as mcal
import pytz
from mysql.connector import Error
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive"
}

def is_a_stock(code):
    """判断是否属于需要采集的A股"""
    # 处理带市场前缀的情况（如sh600000）
    pure_code = code[-6:] if len(code) > 6 else code
    return pure_code.startswith(('600','601','603','605','000','001','002','003','300','301'))

# numeric_cols = ["f2", "f3", "f4", "f5", "f6", "f7", "f8", "f10", "f15", "f16", "f17", "f18", "f22", "f11", "f24", "f25", "f9", "f115", "f114", "f23", "f112", "f113", "f61", "f48", "f37", "f49", "f57", "f40", "f41", "f45", "f46", "f38", "f39", "f20", "f21" ]
# date_cols = ["f26", "f221"]



# 基础字段结构（新增'en'字段）
BASE_COLUMNS = {
                'id' : {'type': INT, 'cn': 'id', 'size': 0,'map': None, 'en': 'id'},
                'date':    {'type': DATE,  'cn': '日期', 'en': 'date', 'map': 0, 'size': 90},
                'date_int':    {'type': INT,  'cn': '日期_int', 'en': 'date_int', 'map': None, 'size': 90},
                'name': {'type': VARCHAR(20), 'cn': '名称', 'size': 120, 'en': 'name'},
                'code': {'type': VARCHAR(6), 'cn': '代码', 'size': 70, 'en': 'code'},
                'code_int': {'type': INT, 'cn': '代码', 'size': 70,'map': None, 'en': 'code_int'},
                'open':    {'type': FLOAT, 'cn': '开盘价', 'en': 'open', 'map': 1, 'size': 70},
                'close':   {'type': FLOAT, 'cn': '收盘价', 'en': 'close', 'map': 2, 'size': 70},
                'high':    {'type': FLOAT, 'cn': '最高价', 'en': 'high', 'map': 3, 'size': 70},
                'low':     {'type': FLOAT, 'cn': '最低价', 'en': 'low', 'map': 4, 'size': 70},
                'volume':  {'type': FLOAT, 'cn': '成交量', 'en': 'volume', 'map': 5, 'size': 120},
                'amount':  {'type': FLOAT, 'cn': '成交额', 'en': 'amount', 'map': 6, 'size': 120},
                'amplitude': {'type': FLOAT, 'cn': '振幅', 'en': 'amplitude', 'map': 7, 'size': 120},
                'quote_change': {'type': FLOAT, 'cn': '涨跌幅', 'en': 'quote_change', 'map': 8, 'size': 120},
                'ups_downs': {'type': FLOAT, 'cn': '涨跌额', 'en': 'ups_downs', 'map': 9, 'size': 120},
                'turnover': {'type': FLOAT, 'cn': '换手率', 'en': 'turnover', 'map': 10, 'size': 120} ,
                'period': {'type': VARCHAR(20),'cn': '周期','en': 'period', 'size': 120}
    }

def create_hist_data(asset_type: str, freq: str, cn_desc: str) -> dict:
    """创建通用历史数据配置
    asset_type: stock/etf/index
    freq: daily/weekly/monthly
    cn_desc: 中文周期描述
    """
    return {
        'name': f'{asset_type}_hist_{freq}',
        'cn': f'{ASSET_CN_MAP[asset_type]}的{cn_desc}行情数据库',
        'columns': BASE_COLUMNS
    }

# 资产类型映射表（新增）
ASSET_CN_MAP = {
    'cn_stock': '股票',
    'cn_etf': 'ETF',
    'cn_index': '指数'
}

# 股票历史数据配置
CN_STOCK_HIST_DAILY_DATA = create_hist_data('cn_stock', 'daily', '日')
CN_STOCK_HIST_WEEKLY_DATA = create_hist_data('cn_stock', 'weekly', '周')
CN_STOCK_HIST_MONTHLY_DATA = create_hist_data('cn_stock', 'monthly', '月')


# ETF历史数据配置
CN_ETF_HIST_DAILY_DATA = create_hist_data('cn_etf', 'daily', '日')  # 注意变量名修正
CN_ETF_HIST_WEEKLY_DATA = create_hist_data('cn_etf', 'weekly', '周')
CN_ETF_HIST_MONTHLY_DATA = create_hist_data('cn_etf', 'monthly', '月')

# 指数历史数据配置
CN_INDEX_HIST_DAILY_DATA = create_hist_data('cn_index', 'daily', '日')
CN_INDEX_HIST_WEEKLY_DATA = create_hist_data('cn_index', 'weekly', '周')
CN_INDEX_HIST_MONTHLY_DATA = create_hist_data('cn_index', 'monthly', '月')


TABLE_REGISTRY = {

    # 股票历史（新增周期维度）
    # 股票历史
    'cn_stock_hist_daily': CN_STOCK_HIST_DAILY_DATA,
    'cn_stock_hist_weekly': CN_STOCK_HIST_WEEKLY_DATA,
    'cn_stock_hist_monthly': CN_STOCK_HIST_MONTHLY_DATA,

    # ETF历史
    'cn_etf_hist_daily': CN_ETF_HIST_DAILY_DATA,
    'cn_etf_hist_weekly': CN_ETF_HIST_WEEKLY_DATA,
    'cn_etf_hist_monthly': CN_ETF_HIST_MONTHLY_DATA,

    # 指数历史
    'cn_index_hist_daily': CN_INDEX_HIST_DAILY_DATA,
    'cn_index_hist_weekly': CN_INDEX_HIST_WEEKLY_DATA,
    'cn_index_hist_monthly': CN_INDEX_HIST_MONTHLY_DATA,

}


def _get_sql_type(py_type):
    """将 SQLAlchemy 类型转换为数据库原生类型字符串（增强版本）"""
    # 处理实例类型（如 VARCHAR(20)）
    if isinstance(py_type, VARCHAR):
        return f"VARCHAR({py_type.length})"
    elif isinstance(py_type, (DATE, DATETIME)):
        return py_type.__class__.__name__.upper()
    elif isinstance(py_type, FLOAT):
        return "FLOAT"
    elif isinstance(py_type, BIGINT):
        return "BIGINT"
    elif isinstance(py_type, INT):
        return "INT"
    elif isinstance(py_type, TINYINT):
        # return "TINYINT"
        return "BOOLEAN"  # 直接映射为 MySQL 的 BOOLEAN 类型

    # 处理类类型（如 DATE 类）
    if py_type == DATE:
        return "DATE"
    elif py_type == DATETIME:
        return "DATETIME"
    elif py_type == FLOAT:
        return "FLOAT"
    elif py_type == BIGINT:
        return "BIGINT"
    elif py_type == INT:
        return "INT"
    elif py_type == TINYINT:
        return "BOOLEAN"  # 直接映射为 MySQL 的 BOOLEAN 类型
        # return "TINYINT"

    raise ValueError(f"Unsupported type: {py_type}")

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
        CN_STOCK_HIST_DAILY_DATA,
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql(
        "SELECT code, market_id, name FROM cn_stock_info WHERE date = (SELECT MAX(date) FROM cn_stock_info WHERE code_int = 1 ) AND market_id IS NOT NULL",
        conn)
    conn.close()

    # 新增过滤逻辑：只保留目标股票
    stock_df = stock_df[stock_df['code'].apply(is_a_stock)]
    print(f"待采集股票数量：{len(stock_df)}")

    # 准备数据容器
    daily_data = pd.DataFrame()

    # 单线程获取数据（替换多线程）
    for _, row in tqdm(stock_df.iterrows(), total=len(stock_df), desc="获取股票历史数据"):
        code = row['code']
        market_id = row['market_id']
        name = row['name']
        period = "daily"

        data = fetch_single_hist(code, market_id, period, name, "stock", beg, end)
        if data is None:
            continue

        df = data['df']
        if data['period'] == 'daily':
            daily_data = pd.concat([daily_data, df], ignore_index=True)

        # 添加延时
        time.sleep(0.5)  # 每次请求后延时0.5秒

    sync_and_write(CN_STOCK_HIST_DAILY_DATA['name'], daily_data)
    print(f"[Success] 股票历史数据写入完成（日：{len(daily_data)}）")


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
        CN_ETF_HIST_DAILY_DATA,
        # CN_ETF_HIST_WEEKLY_DATA,
        # CN_ETF_HIST_MONTHLY_DATA
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code, market_id, name FROM cn_etf_info WHERE date = (SELECT MAX(date) FROM cn_etf_info WHERE  code_int = 159001) AND market_id IS NOT NULL", conn)
    conn.close()

    # 准备数据容器
    daily_data = pd.DataFrame()

    # 单线程获取数据
    for _, row in tqdm(stock_df.iterrows(), total=len(stock_df), desc="获取基金历史数据"):
        code = row['code']
        market_id = row['market_id']
        name = row['name']
        period = "daily"

        data = fetch_single_hist(code, market_id, period, name, "etf", beg, end)
        if data is None:
            continue

        df = data['df']
        if data['period'] == 'daily':
            daily_data = pd.concat([daily_data, df], ignore_index=True)

        # 添加延时
        time.sleep(0.5)

    sync_and_write(CN_ETF_HIST_DAILY_DATA['name'], daily_data)
    print(f"[Success] 基金历史数据写入完成（日：{len(daily_data)}）")

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
        CN_INDEX_HIST_DAILY_DATA,
        # CN_INDEX_HIST_WEEKLY_DATA,
        # CN_INDEX_HIST_MONTHLY_DATA
    ]:
        create_table_if_not_exists(table_config['name'])

    # 获取股票列表
    conn = DBManager.get_new_connection()
    stock_df = pd.read_sql("SELECT code, market_id, name FROM cn_index_info WHERE date = (SELECT MAX(date) FROM cn_index_info WHERE code_int = 1) AND market_id IS NOT NULL", conn)
    conn.close()

    # 准备数据容器
    daily_data = pd.DataFrame()

    # 单线程获取数据
    for _, row in tqdm(stock_df.iterrows(), total=len(stock_df), desc="获取指数历史数据"):
        code = row['code']
        market_id = row['market_id']
        name = row['name']
        period = "daily"

        data = fetch_single_hist(code, market_id, period, name, "index", beg, end)
        if data is None:
            continue

        df = data['df']
        if data['period'] == 'daily':
            daily_data = pd.concat([daily_data, df], ignore_index=True)

        # 添加延时
        time.sleep(0.5)

    sync_and_write(CN_INDEX_HIST_DAILY_DATA['name'], daily_data)
    print(f"[Success] 指数历史数据写入完成（日：{len(daily_data)}）")


'''
# https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.000001&
ut=fa5fd1943c7b386f172d6893dbfba10b&
fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&
fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&
klt=101&
fqt=1&
beg=0&
end=20500101&
smplmt=460&
lmt=1000000&
_=1748611807385
'''

def fetch_single_hist(code: str, market_id: str, period: str, name: str, data_type: str, beg: str, end: str):
    """通用函数：获取单个代码的历史数据（股票/ETF/指数）"""
    try:
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"

        # 根据数据类型选择配置
        table_config_map = {
            "stock": CN_STOCK_HIST_DAILY_DATA,
            "etf": CN_ETF_HIST_DAILY_DATA,
            "index": CN_INDEX_HIST_DAILY_DATA
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
            "lmt": "1000000",
            "_": int(time.time()*1000)
        }

        # 发送请求与数据处理（与原逻辑一致）
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
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
        df['date_int'] = df['date'].astype(str).str.replace('-', '')
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
sync_and_write(table_name: str, data: pd.DataFrame):同步表结构并写入数据
create_table_if_not_exists(table_name)：检查数据表是否存在，如果不存在，则创建数据库并添加索引
同步表结构(conn, table_name, data_columns)： 动态检查并自动添加表中缺失的字段
sql语句生成器(table_name,data)：带入参数数据表名和数据，生成数据插入语句
execute_raw_sql(sql,params)：执行插入数据表
convert_date_format(input_date: str)：将 yyyy-mm-dd 格式转换为 yyyymmdd 格式
"""

# def sync_and_write(table_name: str, data: pd.DataFrame):
#     """通用函数：同步表结构并写入数据"""
#     conn = DBManager.get_new_connection()
#     try:
#         同步表结构(conn, table_name, data.columns)
#     finally:
#         if conn.is_connected():
#             conn.close()
#     sql_txt = sql语句生成器(table_name, data)
#     if not execute_raw_sql(sql_txt):
#         raise Exception(f"{table_name} 写入失败")

def sync_and_write(table_name: str, data: pd.DataFrame):
    """同步表结构并写入数据（新增索引优化）"""
    if data.empty:
        print(f"[Warning] 表 {table_name} 无数据可写入")
        return

    conn = None
    try:
        conn = DBManager.get_new_connection()
        cursor = conn.cursor()

        # ============== 新增代码开始 ==============
        # 1. 禁用索引（独立事务提交）
        disable_sql = f"ALTER TABLE `{table_name}` DISABLE KEYS;"
        cursor.execute(disable_sql)
        conn.commit()
        print(f"[Optimize] 已禁用 {table_name} 表索引")
        # ============== 新增代码结束 ==============

        # 原有逻辑：同步表结构
        同步表结构(conn, table_name, data.columns)

        # 原有逻辑：生成SQL
        sql_txt = sql语句生成器(table_name, data)

        # ============== 修改位置 ==============
        # 替换原有的 execute_raw_sql 调用
        # 原代码：if not execute_raw_sql(sql_txt)
        if not execute_raw_sql(sql_txt, conn):  # 传入连接对象
            raise Exception(f"{table_name} 写入失败")

        # ============== 新增代码开始 ==============
        # 2. 启用索引（独立事务提交）
        enable_sql = f"ALTER TABLE `{table_name}` ENABLE KEYS;"
        cursor.execute(enable_sql)
        conn.commit()
        print(f"[Optimize] 已重建 {table_name} 表索引")
        # ============== 新增代码结束 ==============

    except Exception as e:
        print(f"[Critical] 数据写入异常: {str(e)}")
        # 异常时强制启用索引
        if conn and conn.is_connected():
            cursor.execute(f"ALTER TABLE `{table_name}` ENABLE KEYS;")
            conn.commit()
        raise
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

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
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `date` DATE,
            `date_int` INT,
            `code_int` INT,
            `code` VARCHAR(6),
            `name` VARCHAR(20)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
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

    # 添加索引
    create_index("idx_date_code_int", ["date_int", "code_int"], is_unique=True)
    create_index("idx_code_int", ["code_int"])
    create_index("idx_date_int", ["date_int"])



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
        table_config = TABLE_REGISTRY.get(table_name, {})
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
                        sql_type = _get_sql_type(col_info['type'])

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


def sql语句生成器(table_name, data, batch_size=5000):
    if 'code' in data.columns:
        data['code_int'] = data['code'].astype(int)

    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date', 'code']
    update_clause = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in data.columns if col not in unique_keys])

    # 分批次处理数据
    sql_batches = []
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        value_rows = []
        for row in batch.itertuples(index=False):
            values = []
            for item in row:
                if pd.isna(item):
                    values.append("NULL")
                elif isinstance(item, (datetime.date, datetime.datetime)):
                    values.append(f"'{item.strftime('%Y-%m-%d')}'")
                elif isinstance(item, (int, float)):
                    values.append(str(item))
                else:
                    cleaned = str(item).replace("'", "''")
                    values.append(f"'{cleaned}'")
            value_rows.append(f"({', '.join(values)})")

        values_str = ',\n'.join(value_rows)
        sql = f"""
            INSERT INTO `{table_name}` ({columns})
            VALUES {values_str}
            ON DUPLICATE KEY UPDATE {update_clause};
        """
        sql_batches.append(sql)

    return '\n'.join(sql_batches)


def execute_raw_sql(sql, conn=None):
    """执行原始SQL（新增连接复用）"""
    # ============== 修改开始 ==============
    # 允许外部传入连接
    if not conn:
        conn = DBManager.get_new_connection()
    if not conn:
        return False
    # ============== 修改结束 ==============

    try:
        cursor = conn.cursor(buffered=True)
        statements = [s.strip() for s in sql.split(';') if s.strip()]

        for statement in statements:
            try:
                cursor.execute(statement)
                if cursor.with_rows:
                    cursor.fetchall()  # 消费结果集
            except Exception as e:
                print(f"执行失败: {statement[:100]}... | 错误: {e}")
                return False
        return True
    except Error as e:
        print(f"数据库错误: {e}")
        return False
    finally:
        # ============== 修改开始 ==============
        # 仅关闭外部创建的连接
        if not conn:  # 只有当conn是本地创建时才关闭
            if conn.is_connected():
                cursor.close()
                conn.close()
        # ============== 修改结束 ==============

# def execute_raw_sql(sql, params=None, max_query_size=1024*1024, batch_size=5000):
#     """改进后的SQL执行函数，解决Commands out of sync问题"""
#     connection = DBManager.get_new_connection()
#     if not connection:
#         return False
#     try:
#         cursor = connection.cursor(buffered=True)  # 使用缓冲游标
#         statements = [s.strip() for s in sql.split(';') if s.strip()]

#         for i in range(0, len(statements), batch_size):
#             batch = statements[i:i+batch_size]
#             for statement in batch:
#                 try:
#                     cursor.execute(statement)
#                     cursor.fetchall()  # 显式消费结果集
#                 except Error as e:
#                     print(f"执行失败: {statement[:50]}... | 错误: {e}")
#                     connection.rollback()
#                     return False
#             connection.commit()  # 每批提交一次
#         return True
#     except Error as e:
#         print(f"数据库错误: {e}")
#         connection.rollback()
#         return False
#     finally:
#         if connection.is_connected():
#             cursor.close()
#             connection.close()


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
