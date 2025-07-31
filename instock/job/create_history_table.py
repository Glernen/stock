#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# import os.path
# import sys

# cpath_current = os.path.dirname(os.path.dirname(__file__))
# cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
# sys.path.append(cpath)

import mysql.connector
from mysql.connector import Error
# from instock.lib.database import db_host, db_user, db_password, db_database, db_charset


class DBManager:
    @staticmethod
    def get_new_connection():
        """创建并返回一个新的数据库连接"""
        try:
            connection = mysql.connector.connect(
                # host=db_host,
                # user=db_user,
                # password=db_password,
                # database=db_database,
                # charset=db_charset
                host="rm-uf6kkirh7qc88ug36fo.mysql.rds.aliyuncs.com",
                user="aheng",
                password="Admin888dashabi",
                database="instockdb",
                charset="utf8mb4",
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
    """创建实时股票数据表"""
    # 腾讯实时股票表
    realtime_stock_tx_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_stock_tx` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '股票代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '股票名称',
        `收盘价` FLOAT COMMENT '收盘价',
        `成交量(手)` FLOAT COMMENT '成交量(手)',
        `换手率(%)` FLOAT COMMENT '换手率(%)',
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `振幅(%)` FLOAT COMMENT '振幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `量比` FLOAT COMMENT '量比',
        `涨速(%)` FLOAT COMMENT '涨速(%)',
        `主力净流入(万元)` FLOAT COMMENT '主力净流入(万元)',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        `5日涨跌幅(%)` FLOAT COMMENT '5日涨跌幅(%)',
        `10日涨跌幅(%)` FLOAT COMMENT '10日涨跌幅(%)',
        `20日涨跌幅(%)` FLOAT COMMENT '20日涨跌幅(%)',
        `60日涨跌幅(%)` FLOAT COMMENT '60日涨跌幅(%)',
        `52周涨跌幅(%)` FLOAT COMMENT '52周涨跌幅(%)',
        `年初至今涨跌幅(%)` FLOAT COMMENT '年初至今涨跌幅(%)',
        `主力流出(万元)` FLOAT COMMENT '主力流出(万元)',
        `主力流入(万元)` FLOAT COMMENT '主力流入(万元)',
        `5日主力流出(万元)` FLOAT COMMENT '5日主力流出(万元)',
        `5日主力流入(万元)` FLOAT COMMENT '5日主力流入(万元)',
        `总市值(亿元)` FLOAT COMMENT '总市值(亿元)',
        `流通市值(亿元)` FLOAT COMMENT '流通市值(亿元)',
        `动态市盈率` FLOAT COMMENT '动态市盈率',
        `市净率` FLOAT COMMENT '市净率',
        `股票类型` VARCHAR(20) COMMENT '股票类型',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='腾讯实时股票数据表';
    """

    # 新浪实时股票表
    realtime_stock_sina_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_stock_sina` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '股票代码整数格式',
        `symbol` VARCHAR(20) COMMENT '股票标识',
        `name` VARCHAR(20) COMMENT '股票名称',
        `开盘价` FLOAT COMMENT '开盘价',
        `最高价` FLOAT COMMENT '最高价',
        `最低价` FLOAT COMMENT '最低价',
        `收盘价` FLOAT COMMENT '收盘价',
        `昨收价` FLOAT COMMENT '昨收价',
        `成交量(手)` FLOAT COMMENT '成交量(手)',
        `换手率(%)` FLOAT COMMENT '换手率(%)',
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        `总市值(亿元)` FLOAT COMMENT '总市值(亿元)',
        `流通市值(亿元)` FLOAT COMMENT '流通市值(亿元)',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新浪实时股票数据表';
    """

    DBManager.execute_sql(realtime_stock_tx_sql)
    DBManager.execute_sql(realtime_stock_sina_sql)


if __name__ == "__main__":
    create_tables()