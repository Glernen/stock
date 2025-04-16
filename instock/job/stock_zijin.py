#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)


import datetime
import akshare as ak
import pandas as pd
import pandas_market_calendars as mcal
from tqdm import tqdm
import mysql.connector
from mysql.connector import Error
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset


# 获取股票资金流数据
stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")

# 获取最新交易日
sh_cal = mcal.get_calendar('SSE')
schedule = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today())
latest_trade_date = schedule.index[-1].strftime("%Y-%m-%d") if not schedule.empty else datetime.date.today().strftime("%Y-%m-%d")


def convert_amount(value):
    """将带中文单位的金额转换为整数（单位：元）"""
    if '亿' in value:
        return int(float(value.replace('亿', '')) * 100000000)
    elif '万' in value:
        return int(float(value.replace('万', '')) * 10000)
    else:
        return int(float(value))

# 数据预处理
stock_fund_flow_individual_df = stock_fund_flow_individual_df.assign(
    date=latest_trade_date,
    date_int=int(pd.to_datetime(latest_trade_date).strftime("%Y%m%d")),  # 修改这里
    code=stock_fund_flow_individual_df["股票代码"],
    code_int=stock_fund_flow_individual_df["股票代码"],
    name=stock_fund_flow_individual_df["股票简称"],
    new_price=stock_fund_flow_individual_df["最新价"].astype(float),
    change_rate=stock_fund_flow_individual_df["涨跌幅"].str.replace('%', '').astype(float),
    turnover=stock_fund_flow_individual_df["换手率"].str.replace('%', '').astype(float),
    liuru=stock_fund_flow_individual_df["流入资金"].apply(convert_amount),
    liuchu=stock_fund_flow_individual_df["流出资金"].apply(convert_amount),
    jingliuru=stock_fund_flow_individual_df["净额"].apply(convert_amount)
)[['date', 'date_int', 'code', 'code_int', 'name','new_price','change_rate','turnover','liuru','liuchu','jingliuru']]

print(f'{stock_fund_flow_individual_df}')

class DBManager:
    @staticmethod
    def get_new_connection():
        """创建新的数据库连接"""
        try:
            return mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_database,
                charset=db_charset
            )
        except Error as e:
            print(f"数据库连接失败: {e}")
            return None

    @staticmethod
    def execute_sql(sql: str):
        """执行SQL语句"""
        conn = DBManager.get_new_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                conn.commit()
            except Error as e:
                print(f"SQL执行失败: {e}\nSQL: {sql[:200]}...")
                conn.rollback()
            finally:
                cursor.close()
                conn.close()

def create_table():
    """创建数据表"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS `stock_zijin` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `date` DATE NOT NULL,
        `date_int` INT NOT NULL,
        `code` VARCHAR(10) NOT NULL,
        `code_int` INT NOT NULL,
        `name` VARCHAR(50) NOT NULL,
        `new_price` DECIMAL(10,2) COMMENT '最新价',
        `change_rate` FLOAT COMMENT '涨跌幅（百分比）',
        `turnover` FLOAT COMMENT '换手率（百分比）',
        `liuru` BIGINT COMMENT '流入资金（单位：元）',
        `liuchu` BIGINT COMMENT '流出资金（单位：元）',
        `jingliuru` BIGINT COMMENT '净额（单位：元）',
        UNIQUE KEY `uniq_idx` (`date_int`, `code_int`),
        INDEX `date_idx` (`date`),
        INDEX `code_idx` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    DBManager.execute_sql(create_sql)

def batch_insert_data(df, batch_size=500):
    """批量插入数据"""
    columns = ['date', 'date_int', 'code', 'code_int', 'name', 
              'new_price', 'change_rate', 'turnover', 'liuru', 'liuchu', 'jingliuru']
    
    # 使用参数化占位符
    sql_template = """
    INSERT INTO `stock_zijin` ({})
    VALUES {}
    ON DUPLICATE KEY UPDATE 
        new_price=VALUES(new_price),
        change_rate=VALUES(change_rate),
        turnover=VALUES(turnover),
        liuru=VALUES(liuru),
        liuchu=VALUES(liuchu),
        jingliuru=VALUES(jingliuru);
    """.format(', '.join(columns), '{}')

    for i in tqdm(range(0, len(df), batch_size), desc="插入进度"):
        batch = df.iloc[i:i+batch_size]
        values = []
        for _, row in batch.iterrows():
            # 确保数值类型不带引号，字符串带引号
            val = (
                f"'{row['date']}'", 
                row['date_int'],
                f"'{row['code']}'", 
                row['code_int'],
                f''' '{row['name'].replace("'", "''")}' ''',
                row['new_price'],  # 数值类型直接写入
                row['change_rate'],
                row['turnover'],
                row['liuru'],
                row['liuchu'],
                row['jingliuru']
            )
            values.append(f"({', '.join(map(str, val))})")
        
        if values:
            sql = sql_template.format(', '.join(values))
            DBManager.execute_sql(sql)

def main():
    # 创建数据表
    create_table()
    
    # 执行批量插入
    batch_insert_data(stock_fund_flow_individual_df)
    
    print("数据同步完成")

if __name__ == '__main__':
    main()