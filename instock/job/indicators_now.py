#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 
''' 
2025年6月21日 
1、只计算KDJ，BOLL，WR，CCI，OBV数据；
2、去除CCI84为空的数据(该计算值至少需要84个交易日的数据)；
3、计算窗口值 MAX_HISTORY_WINDOW 改为 60

'''

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import pandas as pd
import numpy as np
# 替换 import talib as tl
import ta
import mysql.connector
import datetime
import threading
from typing import Optional  # 新增导入
# from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
from mysql.connector import Error
# from instock.lib.database import DBManager
import sqlalchemy
import time
import instock.core.tablestructure as tbs
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset



def calculate_indicators(data):
    # 检查数据长度是否满足最小窗口（例如MACD需要至少34条数据）
    min_window = 34  # 根据TA-Lib指标要求调整
    if len(data) < min_window:
        print(f"[WARNING] 数据不足{min_window}条，无法计算指标")
        return pd.DataFrame()

    daily_data_indicators = pd.DataFrame()
    data = data.sort_values(by='date', ascending=True)
   # 原始字段
    daily_data_indicators['date'] = data['date']
    daily_data_indicators['code'] = data['code']


    # 格式化日期为 YYYYMMDD
    daily_data_indicators['date_int'] = data['date'].astype(str).str.replace('-', '')

    if 'code_int' in data.columns:
        daily_data_indicators['code_int'] = data['code_int']
    if 'name' in data.columns:
        daily_data_indicators['name'] = data['name']
    daily_data_indicators['close'] = data['close']
    '''
    计算ETF数据的各种指标。

    参数:
    data (pd.DataFrame): 包含ETF每日数据的DataFrame，至少包含'open' ,'close', 'high', 'low', 'volume'列 收盘价，最高价，最低价，成交量

    返回:
    pd.DataFrame: 包含计算后指标数据的DataFrame
    '''


    # 计算KDJ的K和D
    # daily_data_indicators['kdjk'], daily_data_indicators['kdjd'] = tl.STOCH(
    #     data['high'],
    #     data['low'],
    #     data['close'],
    #     fastk_period=9,    # 默认参数需显式指定
    #     slowk_period=5,
    #     slowk_matype=1,
    #     slowd_period=5,
    #     slowd_matype=1
    # )

    # 使用ta库计算指标
    # 1. 计算MACD
    macd = ta.trend.MACD(close=data['close'])
    daily_data_indicators['macd'] = macd.macd()
    daily_data_indicators['macds'] = macd.macd_signal()
    daily_data_indicators['macdh'] = macd.macd_diff()
    
    # 2. 计算KDJ
    stoch = ta.momentum.StochasticOscillator(
        high=data['high'],
        low=data['low'],
        close=data['close'],
        window=9,
        smooth_window=5
    )
    daily_data_indicators['kdjk'] = stoch.stoch()
    daily_data_indicators['kdjd'] = stoch.stoch_signal()
    daily_data_indicators['kdjj'] = 3 * daily_data_indicators['kdjk'] - 2 * daily_data_indicators['kdjd']
    
    # 3. 计算BOLL
    boll = ta.volatility.BollingerBands(close=data['close'], window=20, window_dev=2)
    daily_data_indicators['boll_ub'] = boll.bollinger_hband()
    daily_data_indicators['boll'] = boll.bollinger_mavg()
    daily_data_indicators['boll_lb'] = boll.bollinger_lband()
    
    # 4. 计算WR
    daily_data_indicators['wr_6'] = ta.momentum.WilliamsRIndicator(
        high=data['high'], low=data['low'], close=data['close'], lbp=6
    ).williams_r()
    
    daily_data_indicators['wr_10'] = ta.momentum.WilliamsRIndicator(
        high=data['high'], low=data['low'], close=data['close'], lbp=10
    ).williams_r()
    
    daily_data_indicators['wr_14'] = ta.momentum.WilliamsRIndicator(
        high=data['high'], low=data['low'], close=data['close'], lbp=14
    ).williams_r()
    
    # 5. 计算CCI
    daily_data_indicators['cci'] = ta.trend.CCIIndicator(
        high=data['high'], low=data['low'], close=data['close'], window=20
    ).cci()
    
    # 6. 计算OBV
    daily_data_indicators['obv'] = ta.volume.OnBalanceVolumeIndicator(
        close=data['close'], volume=data['volume']
    ).on_balance_volume()
    
    return daily_data_indicators




# 表映射配置
TABLE_MAP = {
    'stock': {
        'hist_table': 'cn_stock_hist_daily',
        'info_table': 'cn_stock_info'
    },
    'etf': {
        'hist_table': 'cn_etf_hist_daily',
        'info_table': 'cn_etf_info'
    },
    'index': {
        'hist_table': 'cn_index_hist_daily',
        'info_table': 'cn_index_info'
    }
}

INDICATOR_TABLES = {
    'stock': 'cn_stock_indicators',
    'etf': 'cn_etf_indicators',
    'index': 'cn_index_indicators'
}

# 数据库连接配置
MAX_HISTORY_WINDOW = 60  # 指标计算所需最大历史窗口


def get_latest_codes(data_type: str) -> List[str]:
    """获取指定类型的最新代码列表"""
    try:
        with DBManager.get_new_connection() as conn:
            query = f"""
                SELECT code_int
                FROM {TABLE_MAP[data_type]['info_table']}
                WHERE date = (SELECT MAX(date) FROM {TABLE_MAP[data_type]['info_table']})
            """
            return pd.read_sql(query, conn)['code_int'].tolist()
    except Exception as e:
        print(f"获取{data_type}代码失败：{str(e)}")
        return []

def get_hist_data(code: int, data_type: str, last_date: str = None) -> pd.DataFrame:
    """获取带日期范围的行情数据"""
    try:
        with DBManager.get_new_connection() as conn:
            base_query = f"""
                SELECT * FROM {TABLE_MAP[data_type]['hist_table']}
                WHERE code_int = '{code}'
            """

            if last_date:
                query = f"""
                    {base_query}
                    AND date >= (
                        SELECT DATE_SUB('{last_date}', INTERVAL {MAX_HISTORY_WINDOW} DAY)
                        FROM DUAL
                    )
                """
            else:
                query = base_query + " ORDER BY date ASC LIMIT 1000"

            data = pd.read_sql(query, conn)
            # --- 调试7: 验证原始数据质量 ---
            # print(f"[DEBUG] {code} 原始数据统计:")
            # print("记录数:", len(data))
            # print("时间范围:", data['date'].min(), "至", data['date'].max())
            # print("缺失值统计:")
            # print(data[['close', 'high', 'low', 'volume']].isnull().sum())

            return data.sort_values('date', ascending=True) if not data.empty else pd.DataFrame()
            # return data.sort_values('date', ascending=True)
    except Exception as e:
        print(f"获取{data_type}历史数据失败：{code_int}-{str(e)}")
        return pd.DataFrame()

def calculate_and_save(code: str, data_type: str):
    """完整的处理流水线"""
    try:
        # 检查目标表是否存在
        table_name = INDICATOR_TABLES[data_type]
        # create_table_if_not_exists(table_name)

        # 获取最新处理日期
        last_processed_date = get_last_processed_date(table_name, code)

        # 获取历史数据（增量逻辑）
        hist_data = get_hist_data(code, data_type, last_processed_date)
        if hist_data.empty:
            print(f"跳过空数据：{data_type} {code}")
            return

        # 检查必需字段是否存在
        required_columns = {'date', 'code','open', 'close', 'high', 'low', 'volume'}
        missing_columns = required_columns - set(hist_data.columns)
        if missing_columns:
            print(f"数据缺失关键列 {missing_columns}，跳过处理：{code}")
            return

        # 计算指标
        # print(f"\n=== 开始处理 {code} ===")
        indicators = calculate_indicators(hist_data)

        # --- 调试5: 输出前5行数据样本 ---
        # print(f"[DEBUG] {code} 计算结果样本:")
        # print(indicators.head())

        # --- 调试6: 检查是否存在负无穷或零值 ---
        # print(f"[DEBUG] {code} 异常值统计:")
        # print("Inf values:", (indicators == np.inf).sum().sum())
        # print("-Inf values:", (indicators == -np.inf).sum().sum())
        # print("Zero values:", (indicators == 0).sum().sum())

        # 过滤已存在数据
        if last_processed_date:
            indicators = indicators[indicators['date'] >= last_processed_date]

        # 写入数据库前检查数据是否为空
        # if not indicators.empty:
        #     # 新增过滤条件：删除 cci_84 为0的行
        #     if 'cci_84' in indicators.columns:
        #         indicators = indicators[indicators['cci_84'] != 0]
        #     if not indicators.empty:
        #         sync_and_save(table_name, indicators)
        #         print(f"更新{data_type}指标：{code} {len(indicators)}条")


        if not indicators.empty:
            sync_and_save(table_name, indicators)
            print(f"更新{data_type}指标：{code} {len(indicators)}条")
        else:
            print(f"无新数据需更新：{code}")
    except Exception as e:
        print(f"处理{data_type} {code}失败：{str(e)}")

def get_latest_codes(data_type: str) -> List[int]:
    """获取指定类型的最新代码列表（返回整数列表）"""
    try:
        with DBManager.get_new_connection() as conn:
            query = f"""
                SELECT code_int
                FROM {TABLE_MAP[data_type]['info_table']}
                WHERE date = (SELECT MAX(date) FROM {TABLE_MAP[data_type]['info_table']})
            """
            df = pd.read_sql(query, conn)
            return df['code_int'].astype(int).tolist()  # 强制转换为整数列表
    except Exception as e:
        print(f"获取{data_type}代码失败：{str(e)}")
        return []

def get_last_processed_date(table: str, code: int) -> str:
    """获取指定代码的最后处理日期"""
    try:
        with DBManager.get_new_connection() as conn:
            query = f"""
                SELECT MAX(date) AS last_date
                FROM {table}
                WHERE code_int = '{code}'
            """
            result = pd.read_sql(query, conn)
            return result.iloc[0]['last_date']
    except:
        return None


def get_last_processed_dates_batch(table: str, codes: List[int]) -> Dict[int, str]:
    """批量获取多个代码的最后处理日期"""
    if not codes:
        return {}

    try:
        with DBManager.get_new_connection() as conn:
            code_list = ",".join(map(str, codes))
            query = f"""
                SELECT code_int, MAX(date) AS last_date
                FROM {table}
                WHERE code_int IN ({code_list})
                GROUP BY code_int
            """
            df = pd.read_sql(query, conn)
            return df.set_index('code_int')['last_date'].to_dict()
    except Exception as e:
        print(f"获取最后处理日期失败：{str(e)}")
        return {}


def sync_and_save(table_name: str, data: pd.DataFrame):
    sql_txt = sql语句生成器(table_name, data)
    execute_raw_sql(sql_txt)


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
    create_index("idx_unique_int", ["date_int", "code_int"], is_unique=True)
    create_index("idx_code_int", ["code_int"])
    create_index("idx_date_int", ["date_int"])






def sql语句生成器(table_name, data):
    # # 准备模板
    # sql_template = """INSERT INTO `{table_name}` ({columns}) VALUES {values} ON DUPLICATE KEY UPDATE {update_clause};"""
    # # 预处理列名和更新子句
    # columns = ', '.join([f"`{col}`" for col in data.columns])
    # update_clause = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in data.columns if col not in ['date', 'code']])
    # 准备模板
    #
    #
    # 检查输入数据是否为空
    if data.empty:
        print("[WARNING] 输入数据为空，跳过SQL生成")
        return ""

    # 预处理数据（如添加code_int）
    if 'code' in data.columns and 'code_int' not in data.columns:
        data.insert(0, 'code_int', data['code'].astype(int))

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
    """改进批量写入性能"""
    if not sql.strip():
        print("[WARNING] SQL语句为空，跳过执行")
        return False

    connection = DBManager.get_new_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor()
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        if not statements:
            print("[WARNING] 无有效SQL语句")
            return False

        for statement in statements:
            cursor.execute(statement)
        connection.commit()
        return True
    except Error as e:
        print(f"数据库错误: {e}")
        connection.rollback()
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()



from itertools import islice

def batch(iterable, batch_size=100):
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch

def check_if_first_run() -> bool:
    """检查是否为首次运行（所有指标表无数据或表不存在）"""
    for data_type in ['stock', 'etf', 'index']:
        table_name = INDICATOR_TABLES[data_type]
        try:
            with DBManager.get_new_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}';")
                exists = cursor.fetchone() is not None
                if exists:
                    # 表存在，检查是否有数据
                    query = f"SELECT 1 FROM `{table_name}` LIMIT 1"
                    result = pd.read_sql(query, conn)
                    if not result.empty:
                        return False  # 存在数据，非首次运行
                else:
                    # 表不存在，属于首次运行
                    return True
        except Exception as e:
            print(f"检查表 {table_name} 失败：{str(e)}")
            return True
    return True


def sync_table_structure(table_name: str, data_columns: List[str]):
    """根据实际数据字段动态同步表结构"""
    try:
        with DBManager.get_new_connection() as conn:
            cursor = conn.cursor()

            # 1. 创建表（如果不存在）
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                # 基础表结构（date, code_int, code, name）
                create_sql = f"""
                    CREATE TABLE `{table_name}` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                        `date` DATE,
                        `date_int` INT,
                        `code_int` INT,
                        `code` VARCHAR(6),
                        `name` VARCHAR(20),
                        INDEX `idx_date_int` (`date_int`),
                        INDEX `idx_code_int` (`code_int`),
                        INDEX `idx_unique_int` (`code_int`,`date_int`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
                """
                cursor.execute(create_sql)
                print(f"创建基础表 {table_name}")

            # 2. 动态添加指标字段
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
                  AND TABLE_SCHEMA = DATABASE()
            """)
            existing_columns = {row[0] for row in cursor.fetchall()}

            # 3. 遍历指标字段，添加缺失列
            for col in data_columns:
                if col not in existing_columns and col not in ['id', 'date','date_int', 'code_int', 'code', 'name']:
                    # 自动推断字段类型（假设均为FLOAT）
                    alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` FLOAT;"
                    cursor.execute(alter_sql)
                    print(f"动态添加字段 {col} 到表 {table_name}")
            conn.commit()
    except Exception as e:
        print(f"同步表 {table_name} 结构失败：{str(e)}")
        sys.exit(1)


def get_hist_data_batch(batch_codes: List[int], data_type: str) -> pd.DataFrame:
    """严格按批次执行单次查询（无分块），获取最近MAX_HISTORY_WINDOW天数据"""
    if not batch_codes:
        return pd.DataFrame()

    try:
        with DBManager.get_new_connection() as conn:
            code_list = ",".join(map(str, batch_codes))
            
            # 计算日期范围
            date_condition = f"date >= CURDATE() - INTERVAL {MAX_HISTORY_WINDOW} DAY"
            
            query = f"""
                SELECT date, date_int, code, code_int, name, open, close, high, low, volume
                FROM {TABLE_MAP[data_type]['hist_table']}
                WHERE code_int IN ({code_list})
                  AND {date_condition}
                ORDER BY code_int, date_int ASC 
            """
            return pd.read_sql(query, conn)
    except Exception as e:
        print(f"获取批次数据失败：{str(e)}")
        return pd.DataFrame()

# def get_hist_data_batch(batch_codes: List[int], data_type: str) -> pd.DataFrame:
#     """严格按批次执行单次查询（无分块）"""
#     if not batch_codes:
#         return pd.DataFrame()

#     try:
#         with DBManager.get_new_connection() as conn:
#             code_list = ",".join(map(str, batch_codes))

#             query = f"""
#                 SELECT *
#                 FROM {TABLE_MAP[data_type]['hist_table']}
#                 WHERE code_int IN ({code_list})
#                 -- ORDER BY code_int, date DESC
#             """
#             return pd.read_sql(query, conn)
#     except Exception as e:
#         print(f"获取批次数据失败：{str(e)}")
#         return pd.DataFrame()


def calculate_and_save_batch(code: int, data_type: str, batch_data: pd.DataFrame) -> pd.DataFrame:
    """从批次数据中提取单个代码数据（无数据库交互）"""
    try:
        # 直接从批次数据过滤
        hist_data = batch_data[batch_data['code_int'] == code].copy()
        if hist_data.empty:
            print(f"跳过空数据：{data_type} {code}")
            return pd.DataFrame()

        # 后续处理逻辑
        table_name = INDICATOR_TABLES[data_type]
        last_processed_date = get_last_processed_date(table_name, code)

        # 检查必需字段
        required_columns = {'date', 'code', 'open', 'close', 'high', 'low', 'volume'}
        missing_columns = required_columns - set(hist_data.columns)
        if missing_columns:
            print(f"数据缺失关键列 {missing_columns}，跳过处理：{code}")
            return pd.DataFrame()

        # 计算指标
        indicators = calculate_indicators(hist_data)
        if indicators.empty:
            return pd.DataFrame()

        # 过滤已处理日期
        if last_processed_date:
            indicators = indicators[indicators['date'] >= last_processed_date]

        # 过滤无效cci_84
        # if 'cci_84' in indicators.columns:
        #     indicators = indicators[indicators['cci_84'] != 0]

        return indicators if not indicators.empty else pd.DataFrame()
    except Exception as e:
        print(f"处理{data_type} {code}失败：{str(e)}")
        return pd.DataFrame()

def process_single_code(
    code: int,
    data_type: str,
    code_data: pd.DataFrame,
    last_processed_date: Optional[str] = None
) -> pd.DataFrame:
    """处理单个代码的计算逻辑（完全基于传入的code_data）"""
    try:
        # 检查数据量是否足够（34条为TA-Lib最低要求）
        if len(code_data) < 34:
            print(f"代码 {code} 数据不足34条（实际{len(code_data)}条），跳过")
            return pd.DataFrame()

        # 计算指标
        indicators = calculate_indicators(code_data)
        if indicators.empty:
            return pd.DataFrame()

        # 过滤已处理日期
        # 使用预取的last_processed_date过滤数据
        if last_processed_date:
            indicators = indicators[indicators['date'] >= last_processed_date]


        # 过滤无效cci_84
        # if 'cci_84' in indicators.columns:
        #     indicators = indicators[indicators['cci_84'] != 0]

        return indicators if not indicators.empty else pd.DataFrame()
    except Exception as e:
        print(f"处理代码 {code} 失败：{str(e)}")
        return pd.DataFrame()


def main():
    start_time = time.time()
    try:
        '''
        # 检查是否为首次运行（任一指标表无数据）
        is_first_run = check_if_first_run()

        # 首次运行时动态同步表结构
        if is_first_run:
            # 定义每个类型的示例code_int
            sample_codes = {
                'stock': 1,      # 假设code_int=1为有效股票
                'etf': 159001,   # 假设code_int=159001为有效ETF
                'index': 1       # 假设code_int=1为有效指数
            }

            for data_type in ['stock', 'etf', 'index']:
                table_name = INDICATOR_TABLES[data_type]
                code_int = sample_codes[data_type]

                # create_table_if_not_exists(table_name)  # 确保只执行一次

                # 1. 获取足够的历史数据（至少34条）
                # 获取历史数据（直接传递整数）
                hist_data = get_hist_data(code_int, data_type, last_date=None)
                if len(hist_data) < 34:
                    print(f"错误：{data_type}示例数据不足34条（当前{len(hist_data)}条），无法同步结构！")
                    sys.exit(1)

                # 2. 计算指标，获取所有字段
                indicators = calculate_indicators(hist_data)
                if indicators.empty:
                    print(f"错误：{data_type}指标计算失败！")
                    sys.exit(1)

                # 3. 动态同步表结构（基于实际字段）
                sync_table_structure(table_name, indicators.columns)

            print("首次运行表结构同步完成")
        '''



        batch_size = 500
        max_workers = 10

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for data_type in ['etf', 'index', 'stock']:
                codes = get_latest_codes(data_type)
                print(f"开始处理 {data_type} 共 {len(codes)} 个代码")

                for batch_idx in range(0, len(codes), batch_size):
                    batch_codes = codes[batch_idx:batch_idx + batch_size]
                    print(f"处理批次 {batch_idx//batch_size+1}，代码数：{len(batch_codes)}")

                    # 1. 获取本批次历史数据
                    batch_data = get_hist_data_batch(batch_codes, data_type)
                    if batch_data.empty:
                        print(f"批次 {batch_idx//batch_size+1} 无数据，跳过")
                        continue

                    # 2. 批量获取最后处理日期（关键修改点）
                    # 从本批数据中提取所有唯一代码
                    unique_codes_in_batch = batch_data['code_int'].unique().tolist()
                    last_dates_map = get_last_processed_dates_batch(
                        INDICATOR_TABLES[data_type],
                        unique_codes_in_batch
                    )

                    # 3. 并行处理本批次代码
                    futures = []
                    for code in batch_codes:
                        # 从批次数据中提取单个代码数据
                        code_data = batch_data[batch_data['code_int'] == code].copy()
                        if code_data.empty:
                            continue
                        # 提交任务时传入预取的最后处理日期
                        futures.append(executor.submit(
                            process_single_code,
                            code=code,
                            data_type=data_type,
                            code_data=code_data,
                            last_processed_date=last_dates_map.get(code, None)
                        ))

                    # 4. 合并并提交本批次结果
                    valid_dfs = []
                    for future in as_completed(futures):
                        df = future.result()
                        if df is not None and not df.empty:
                            valid_dfs.append(df)

                    if valid_dfs:
                        combined_data = pd.concat(valid_dfs, ignore_index=True)
                        sync_and_save(INDICATOR_TABLES[data_type], combined_data)
                        print(f"批次提交成功，记录数：{len(combined_data)}")
                    else:
                        print(f"本批次无有效数据")
    finally:
        print(f"\n🕒 总耗时: {time.time()-start_time:.2f}秒")  # 确保异常时也输出

if __name__ == "__main__":
    main()
