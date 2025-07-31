#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# import os.path
# import sys

# cpath_current = os.path.dirname(os.path.dirname(__file__))
# cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
# sys.path.append(cpath)

import mysql.connector
from mysql.connector import Error
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


def create_tables():
    """创建K线数据表"""
    # 股票K线图形表
    pattern_stock_sql = """
    CREATE TABLE IF NOT EXISTS `pattern_stock` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '股票代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '股票名称',
        `open` FLOAT COMMENT '开盘价',
        `close` FLOAT COMMENT '收盘价',
        `high` FLOAT COMMENT '最高价',
        `low` FLOAT COMMENT '最低价',
        `volume` FLOAT COMMENT '成交量（手）',
        `turnoverrate` FLOAT COMMENT '换手率',
        `阳锤子线` FLOAT COMMENT '阳锤子线',
        `阴锤子线` FLOAT COMMENT '阴锤子线',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票K线图形表';
    """

    # 指数K线图形表
    pattern_index_sql = """
    CREATE TABLE IF NOT EXISTS `pattern_index` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '指数代码',
        `code_int` INT NOT NULL COMMENT '指数代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '指数名称',
        `open` FLOAT COMMENT '开盘价',
        `close` FLOAT COMMENT '收盘价',
        `high` FLOAT COMMENT '最高价',
        `low` FLOAT COMMENT '最低价',
        `volume` FLOAT COMMENT '成交量（手）',
        `turnoverrate` FLOAT COMMENT '换手率',
        `阳锤子线` FLOAT COMMENT '阳锤子线',
        `阴锤子线` FLOAT COMMENT '阴锤子线',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数K线图形表';
    """


    # 基金K线图形表
    pattern_etf_sql = """
    CREATE TABLE IF NOT EXISTS `pattern_etf` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '基金代码',
        `code_int` INT NOT NULL COMMENT '基金代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '基金名称',
        `open` FLOAT COMMENT '开盘价',
        `close` FLOAT COMMENT '收盘价',
        `high` FLOAT COMMENT '最高价',
        `low` FLOAT COMMENT '最低价',
        `volume` FLOAT COMMENT '成交量（手）',
        `turnoverrate` FLOAT COMMENT '换手率',
        `阳锤子线` FLOAT COMMENT '阳锤子线',
        `阴锤子线` FLOAT COMMENT '阴锤子线',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基金K线图形表';
    """


    # 行业K线图形表
    pattern_industry_sql = """
    CREATE TABLE IF NOT EXISTS `pattern_industry` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '行业代码',
        `code_int` INT NOT NULL COMMENT '行业代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '行业名称',
        `open` FLOAT COMMENT '开盘价',
        `close` FLOAT COMMENT '收盘价',
        `high` FLOAT COMMENT '最高价',
        `low` FLOAT COMMENT '最低价',
        `volume` FLOAT COMMENT '成交量（手）',
        `turnoverrate` FLOAT COMMENT '换手率',
        `阳锤子线` FLOAT COMMENT '阳锤子线',
        `阴锤子线` FLOAT COMMENT '阴锤子线',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行业K线图形表';
    """


    # 股票K线图形表
    DBManager.execute_sql(pattern_stock_sql)
    # 指数K线图形表
    DBManager.execute_sql(pattern_index_sql)
    # 基金K线图形表
    DBManager.execute_sql(pattern_etf_sql)
    # 行业K线图形表
    DBManager.execute_sql(pattern_industry_sql)


if __name__ == "__main__":
    create_tables()