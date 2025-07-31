#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import logging
import concurrent.futures
import pandas as pd
import numpy as np
import os.path
import sys
import datetime
import mysql.connector
from sqlalchemy import create_engine, DATE, FLOAT, VARCHAR, INT
from mysql.connector import Error
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset

# 数据库配置
db_config = {
    'host': db_host,
    'user': db_user,
    'password': db_password,
    'database': db_database,
    'charset': 'utf8mb4'
}

# 表结构配置
RATE_FIELDS_COUNT = 100

TABLE_CN_STOCK_INDICATORS_BUY = {
    'name': 'cn_stock_indicators_buy',
    'columns': {
        'date': {'type': DATE},
        'date_int': {'type': INT},
        'code': {'type': VARCHAR(6)},
        'code_int': {'type': INT},
        'name': {'type': VARCHAR(20)},
        'strategy': {'type':VARCHAR(50) },
        'close': {'type':FLOAT },
        'kdjj': {'type':FLOAT },
        'turnover': {'type':FLOAT },
        'jingliuru_cn': {'type':VARCHAR(50) },
        'industry': {'type':VARCHAR(50) },
        'up_sentiment': {'type':INT },
        'down_sentiment': {'type':INT },
        'industry_kdjj': {'type':FLOAT },
        'industry_kdjj_day1': {'type':FLOAT },
        'industry_kdj': {'type':VARCHAR(50) },
        'industry_wr': {'type':VARCHAR(50) },
        'industry_cci': {'type':VARCHAR(50) },
        'industry_sentiment': {'type':VARCHAR(50) },
        **{f'rate_{i}': {'type': FLOAT} for i in range(1, RATE_FIELDS_COUNT+1)}
    }
}

TABLE_STRATEGY_STOCK_BUY_OPTIMIZATION = {
    'name': 'strategy_stock_buy_optimization',
    'columns': {
        'date': {'type': DATE},
        'date_int': {'type': INT},
        'code': {'type': VARCHAR(6)},
        'code_int': {'type': INT},
        'name': {'type': VARCHAR(20)},
        'strategy': {'type':VARCHAR(50) },
        'close': {'type':FLOAT },
        'kdjj': {'type':FLOAT },
        'turnover': {'type':FLOAT },
        'jingliuru_cn': {'type':VARCHAR(50) },
        'industry': {'type':VARCHAR(50) },
        'up_sentiment': {'type':INT },
        'down_sentiment': {'type':INT },
        'industry_kdjj': {'type':FLOAT },
        'industry_kdjj_day1': {'type':FLOAT },
        'industry_kdj': {'type':VARCHAR(50) },
        'industry_wr': {'type':VARCHAR(50) },
        'industry_cci': {'type':VARCHAR(50) },
        'industry_sentiment': {'type':VARCHAR(50) },
        **{f'rate_{i}': {'type': FLOAT} for i in range(1, RATE_FIELDS_COUNT+1)}
    }
}


TABLE_CN_STOCK_INDICATORS_SELL = {
    'name': 'cn_stock_indicators_sell',
    'columns': {
        'date': {'type': DATE},
        'date_int': {'type': INT},
        'code': {'type': VARCHAR(6)},
        'code_int': {'type': INT},
        'name': {'type': VARCHAR(20)},
        'strategy': {'type':VARCHAR(50) },
        'close': {'type':FLOAT },
        'kdjj': {'type':FLOAT },
        'turnover': {'type':FLOAT },
        'jingliuru_cn': {'type':VARCHAR(50) },
        'industry': {'type':VARCHAR(50) },
        'up_sentiment': {'type':INT },
        'down_sentiment': {'type':INT },
        'industry_kdjj': {'type':FLOAT },
        'industry_kdjj_day1': {'type':FLOAT },
        'industry_kdj': {'type':VARCHAR(50) },
        'industry_wr': {'type':VARCHAR(50) },
        'industry_cci': {'type':VARCHAR(50) },
        'industry_sentiment': {'type':VARCHAR(50) },
        **{f'rate_{i}': {'type': FLOAT} for i in range(1, RATE_FIELDS_COUNT+1)}
    }
}

# 回测字段配置
backtest_columns = [
    'date_int', 'code_int' , 'code', 'date', 'name',
    'strategy', 'close', 'kdjj', 'turnover', 'jingliuru_cn',
    'industry', 'up_sentiment', 'down_sentiment',
    'industry_kdjj', 'industry_kdjj_day1', 'industry_kdj',
    'industry_wr', 'industry_cci', 'industry_sentiment'

] + [f'rate_{i}' for i in range(1, 101)]

class StockHistData:
    def __init__(self):
        self.engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )

    def get_recent_data(self, days=100):
        """获取最近N天的股票历史数据"""
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y%m%d")
        query = f"""
            SELECT * FROM kline_stock
            ORDER BY date_int ASC
        """
        # query = f"""
        #     SELECT * FROM kline_stock
        #     WHERE date_int BETWEEN {start_date} AND {end_date}
        #     ORDER BY date_int ASC
        # """
        try:
            df = pd.read_sql_query(query, self.engine)
            # df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            return df.set_index(['date_int', 'code_int'])
        except Exception as e:
            logging.error(f"数据库查询失败: {str(e)}")
            return None

def prepare():
    # tables = [TABLE_CN_STOCK_INDICATORS_BUY, TABLE_CN_STOCK_INDICATORS_SELL]
    tables = [TABLE_CN_STOCK_INDICATORS_BUY,TABLE_CN_STOCK_INDICATORS_SELL,TABLE_STRATEGY_STOCK_BUY_OPTIMIZATION]

    # 获取历史数据
    hist_data = StockHistData().get_recent_data()
    if hist_data is None:
        return

    # print(f'{hist_data}')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            executor.submit(process_table, table, hist_data)

def process_table(table, hist_data):
    # 添加在 process_table 函数内
    def safe_float(value):
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        try:
            return float(value)
        except:
            return None

    def safe_str(value):
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        try:
            return str(value)
        except:
            return None

    def safe_int(value):
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        try:
            return int(value)
        except:
            return None

    table_name = table['name']

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT date, date_int, code , code_int, name, strategy, close, kdjj, turnover, jingliuru_cn, industry, up_sentiment, down_sentiment, industry_kdjj, industry_kdjj_day1, industry_kdj, industry_wr, industry_cci, industry_sentiment
            FROM {table_name}
            WHERE rate_60 IS NULL
        """)
        stocks = cursor.fetchall()
        # print(f"需处理股票: {stocks}")
        print(f"需处理股票数量: {len(stocks)}")

        results = {}
        for idx, stock in enumerate(stocks, 1):
            try:
                date_int = stock[1]
                code_int = stock[3]
                key = (date_int, code_int)

                # 从预加载数据中筛选当前股票的数据
                stock_data = hist_data.xs(code_int, level='code_int', drop_level=False)
                # print(f"\n股票{code_int}总数据量：{len(stock_data)}条")

                # 找到当前日期在时间序列中的位置
                date_index = stock_data.index.get_loc(key)
                # print(f"当前记录在时间序列中的位置：{date_index}")

                # 获取从当前日期开始往后100天的数据
                needed_data = stock_data.iloc[date_index:date_index+90]
                # print(f"可用于计算的数据量：{len(needed_data)}")

                rates = calculate_rates(stock, needed_data)
                results[key] = rates

            except Exception as e:
                print(f"处理{code_int}时发生异常: {str(e)}")
                continue

        # 更新数据库
        if results:
            print(f"准备更新{len(results)}条记录")
            update_data = []
            for (date_int, code_int), rates in results.items():
                # 关键修改：从results遍历中获取对应股票数据
                current_stock = next((s for s in stocks if s[1] == date_int and s[3] == code_int), None)
                if not current_stock:
                    print(f"未找到匹配的股票数据 date_int={date_int} code_int={code_int}")
                    continue

                # 添加详细日志输出
                # print(f"\n正在构建更新数据：")
                # print(f"日期: {date_int} 代码: {code_int}")
                # print(f"原始数据: {current_stock}")
                # print(f"收益率数据长度: {len(rates)} 前3项: {rates[:3]}")

                # 关键修改1：转换日期格式和数值类型
                formatted_date = current_stock[0].strftime('%Y-%m-%d')  # 转换datetime.date为字符串
                formatted_rates = [float(rate) if rate is not None else None for rate in rates]  # 转换numpy类型

                update_row = [
                     current_stock[2],  # code
                     formatted_date,  # date
                     safe_str(current_stock[4]),  # name
                     safe_str(current_stock[5]) if len(current_stock) > 5 else None,  # strategy
                     safe_float(current_stock[6]) if len(current_stock) > 6 else None,  # close
                     safe_float(current_stock[7]) if len(current_stock) > 7 else None,  # kdjj
                     safe_float(current_stock[8]) if len(current_stock) > 8 else None,  # turnover
                     safe_str(current_stock[9]) if len(current_stock) > 9 else None,  # jingliuru_cn
                     safe_str(current_stock[10]) if len(current_stock) > 10 else None,  # industry
                     safe_int(current_stock[11]) if len(current_stock) > 11 else None,  # up_sentiment
                     safe_int(current_stock[12]) if len(current_stock) > 12 else None,  # down_sentiment
                     safe_float(current_stock[13]) if len(current_stock) > 13 else None,  # industry_kdjj
                     safe_float(current_stock[14]) if len(current_stock) > 14 else None,  # industry_kdjj_day1
                     safe_str(current_stock[15]) if len(current_stock) > 15 else None,  # industry_kdj
                     safe_str(current_stock[16]) if len(current_stock) > 16 else None,  # industry_wr
                     safe_str(current_stock[17]) if len(current_stock) > 17 else None,  # industry_cci
                     safe_str(current_stock[18]) if len(current_stock) > 18 else None,  # industry_sentiment
                    *formatted_rates,   # rate_1~rate_100
                ] + [
                    date_int,  # WHERE条件
                    code_int
                ]

                # print(f"参数数量验证: {len(update_row)} 应等于: {len(backtest_columns[2:])+2}")
                # print(f"示例数据：{update_row[:8]}...{update_row[-3:]}")
                update_data.append(tuple(update_row))

            # 添加SQL调试信息
            print(f"\n生成的UPDATE语句结构：")
            set_clause = ', '.join([f'{col}=%s' for col in backtest_columns[2:]])
            # print(f"SET字段数：{set_clause}")
            print(f"SET子句字段数：{len(backtest_columns[2:])} 实际：{set_clause.count('%s')}")
            print(f"WHERE条件参数数：2")
            print(f"总占位符数：{set_clause.count('%s') + 2}")

            update_query = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE date_int=%s AND code_int=%s
            """
            print(f"update_query{update_query}")
            try:
                print(f"开始执行批量更新，记录数：{len(update_data)}")
                cursor.executemany(update_query, update_data)
                conn.commit()
                print(f"成功更新{len(update_data)}条记录")
            except Error as e:
                print(f"更新失败: {str(e)}")
                # 打印第一条失败记录的参数
                if update_data:
                    print(f"示例参数：{update_data[0]}")
                    print(f"参数数量：{len(update_data[0])}")


    except Error as e:
        print(f"数据库操作失败: {str(e)}")
        if e.errno == 1213:  # Deadlock处理
            conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def calculate_rates(stock, stock_data):
    """计算收益率（处理单行数据）"""
    try:
        if len(stock_data) == 0:
            print("空数据输入")
            return [None] * 100

        # 获取基准收盘价
        base_close = stock_data.iloc[0]['close']

        # 检查基准收盘价是否有效
        if pd.isna(base_close) or base_close == 0:
            print(f"无效基准价: {base_close}，股票: {stock_data.iloc[0]['code']} 日期: {stock_data.iloc[0]['date']}")
            return [None] * 100

        rates = []
        max_days = min(len(stock_data) - 1, 100)

        for i in range(1, max_days + 1):
            row = stock_data.iloc[i]
            current_close = row['close']

            # 检查当前收盘价是否有效
            if pd.isna(current_close):
                rates.append(None)
                continue

            # 安全计算收益率
            try:
                pct = round((current_close - base_close) / base_close * 100, 2)
                rates.append(pct)
            except (ZeroDivisionError, FloatingPointError):
                rates.append(None)
                print(f"计算错误: 股票 {stock_data.iloc[0]['code']} 第{i}天 基准价={base_close} 当前价={current_close}")

        # 补足剩余天数
        rates += [None] * (100 - len(rates))
        return rates[:100]

    except Exception as e:
        logging.error(f"数据索引超出范围,收益率计算失败 {stock[2]}: {str(e)}")
        return [None] * 100


def check_table_exists(table_name):
    """检查表是否存在"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None
    except Error as e:
        logging.error(f"表检查失败: {str(e)}")
        return False
    finally:
        if conn.is_connected():
            conn.close()

def create_table(table):
    """创建表结构"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        columns = []
        for col_name, col_def in table['columns'].items():
            col_type = {
                DATE: "DATE",
                INT: "INT",
                VARCHAR(6): "VARCHAR(6)",
                FLOAT: "FLOAT",
                VARCHAR(20): "VARCHAR(20)"
            }[col_def['type']]
            columns.append(f"{col_name} {col_type}")

        create_sql = f"""
            CREATE TABLE {table['name']} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(columns)},
                INDEX idx_unique (date_int, code_int, strategy)
            )
        """
        cursor.execute(create_sql)
        conn.commit()
    except Error as e:
        logging.error(f"表创建失败: {str(e)}")
    finally:
        if conn.is_connected():
            conn.close()


def main():
    logging.basicConfig(level=logging.INFO)
    prepare()


if __name__ == '__main__':
    main()
