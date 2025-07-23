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
    """创建实时股票数据表"""
    # 腾讯实时股票表
    realtime_stock_tx_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_stock_tx` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '股票代码整数格式',
        `symbol` VARCHAR(20) COMMENT '股票标识',
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
        `市盈率TTM` FLOAT COMMENT '滚动市盈率',
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

    # 东方财富实时股票表
    realtime_stock_df_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_stock_df` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '股票代码整数格式',
        `symbol` VARCHAR(20) COMMENT '股票标识',
        `name` VARCHAR(20) COMMENT '股票名称',
        `市场标识` INT  COMMENT '市场标识',
        `开盘价` FLOAT COMMENT '开盘价',
        `最高价` FLOAT COMMENT '最高价',
        `最低价` FLOAT COMMENT '最低价',
        `收盘价` FLOAT COMMENT '收盘价',
        `昨收价` FLOAT COMMENT '昨收价',
        `成交量(手)` FLOAT COMMENT '成交量(手)',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        `振幅(%)` FLOAT COMMENT '振幅(%)',
        `换手率(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `量比` FLOAT COMMENT '量比',
        `涨速(%)` FLOAT COMMENT '涨速(%)',
        `5分钟涨跌幅(%)` FLOAT COMMENT '5分钟涨跌幅(%)',
        `60日涨跌幅(%)` FLOAT COMMENT '60日涨跌幅(%)',
        `年初至今涨跌幅(%)` FLOAT COMMENT '年初至今涨跌幅(%)',
        `动态市盈率` FLOAT COMMENT '动态市盈率',
        `市盈率TTM` FLOAT COMMENT '滚动市盈率',
        `静态市盈率` FLOAT COMMENT '静态市盈率',
        `市净率` FLOAT COMMENT '市净率',
        `每股收益` FLOAT COMMENT '每股收益',
        `每股净资产` FLOAT COMMENT '每股净资产',
        `每股公积金` FLOAT COMMENT '每股公积金',
        `每股未分配利润` FLOAT COMMENT '每股未分配利润',
        `加权净资产收益率` FLOAT COMMENT '加权净资产收益率',
        `毛利率` FLOAT COMMENT '毛利率',
        `资产负债率` FLOAT COMMENT '资产负债率',
        `营业收入` FLOAT COMMENT '营业收入',
        `营业收入同比增长` FLOAT COMMENT '营业收入同比增长',
        `归属净利润` FLOAT COMMENT '归属净利润',
        `归属净利润同比增长` FLOAT COMMENT '归属净利润同比增长',
        `报告期` DATE COMMENT '报告期',
        `总股本` FLOAT COMMENT '总股本',
        `已流通股份` FLOAT COMMENT '已流通股份',
        `总市值(亿元)` FLOAT COMMENT '总市值(亿元)',
        `流通市值(亿元)` FLOAT COMMENT '流通市值(亿元)',
        `所处行业` VARCHAR(100) COMMENT '所处行业',
        `上市时间` DATE COMMENT '上市时间', 
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='东方财富网实时股票数据表';
    """

    #########################################################

    # 新浪实时指数表
    realtime_index_sina_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_index_sina` (
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
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新浪实时指数数据表';
    """

    # 东方财富网实时指数表
    realtime_index_df_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_index_df` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '指数代码',
        `code_int` INT NOT NULL COMMENT '指数代码整数格式',
        `symbol` VARCHAR(20) COMMENT '指数标识',
        `name` VARCHAR(20) COMMENT '指数名称',
        `市场标识` VARCHAR(20) COMMENT '市场标识',
        `开盘价` FLOAT COMMENT '开盘价',
        `最高价` FLOAT COMMENT '最高价',
        `最低价` FLOAT COMMENT '最低价',
        `收盘价` FLOAT COMMENT '收盘价',
        `昨收价` FLOAT COMMENT '昨收价',
        `成交量(手)` FLOAT COMMENT '成交量(手)',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        `换手率(%)` FLOAT COMMENT '换手率(%)',
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `总市值(亿元)` FLOAT COMMENT '总市值(亿元)',
        `流通市值(亿元)` FLOAT COMMENT '流通市值(亿元)',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='东方财富网实时指数数据表';
    """

    #########################################################

    # 新浪实时基金表
    realtime_etf_sina_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_etf_sina` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '基金代码',
        `code_int` INT NOT NULL COMMENT '基金代码整数格式',
        `symbol` VARCHAR(20) COMMENT '基金标识',
        `name` VARCHAR(20) COMMENT '基金名称',
        `开盘价` FLOAT COMMENT '开盘价',
        `最高价` FLOAT COMMENT '最高价',
        `最低价` FLOAT COMMENT '最低价',
        `收盘价` FLOAT COMMENT '收盘价',
        `昨收价` FLOAT COMMENT '昨收价',
        `成交量(手)` FLOAT COMMENT '成交量(手)',
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新浪实时基金数据表';
    """

    # 东方财富网实时基金表
    realtime_etf_df_sql = """
    CREATE TABLE IF NOT EXISTS `realtime_etf_df` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `date` DATE NOT NULL COMMENT '日期',
        `date_int` INT NOT NULL COMMENT '日期整数格式',
        `code` VARCHAR(6) NOT NULL COMMENT '基金代码',
        `code_int` INT NOT NULL COMMENT '基金代码整数格式',
        `name` VARCHAR(20) COMMENT '基金名称',
        `市场标识` VARCHAR(20) COMMENT '市场标识',
        `开盘价` FLOAT COMMENT '开盘价',
        `最高价` FLOAT COMMENT '最高价',
        `最低价` FLOAT COMMENT '最低价',
        `收盘价` FLOAT COMMENT '收盘价',
        `昨收价` FLOAT COMMENT '昨收价',
        `成交量(手)` FLOAT COMMENT '成交量(手)',
        `成交额(万元)` FLOAT COMMENT '成交额(万元)',
        `换手率(%)` FLOAT COMMENT '换手率(%)',
        `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
        `涨跌额` FLOAT COMMENT '涨跌额',
        `总市值(亿元)` FLOAT COMMENT '总市值(亿元)',
        `流通市值(亿元)` FLOAT COMMENT '流通市值(亿元)',
        UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
        INDEX `idx_date` (`date`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='东方财富网实时基金数据表';
    """

    #########################################################

    # 腾讯实时行业申万一级数据表\申万二级数据表
    realtime_industry_tx_sql = """
     CREATE TABLE IF NOT EXISTS `realtime_industry_tx` (
         `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
         `date` DATE NOT NULL COMMENT '日期',
         `date_int` INT NOT NULL COMMENT '日期整数格式',
         `code` VARCHAR(10) NOT NULL COMMENT '行业代码',
         `code_int` INT NOT NULL COMMENT '行业代码整数格式',
         `symbol` VARCHAR(20) COMMENT '行业标识',
         `name` VARCHAR(20) NOT NULL COMMENT '行业名称',
         `行业类型` VARCHAR(20) COMMENT '行业类型',
         `收盘价` FLOAT COMMENT '收盘价',
         `成交量(手)` FLOAT COMMENT '成交量(手)',
         `换手率(%)` FLOAT COMMENT '换手率(%)',
         `涨跌幅(%)` FLOAT COMMENT '涨跌幅(%)',
         `涨跌额` FLOAT COMMENT '涨跌额',
         `量比` FLOAT COMMENT '量比',
         `涨速(%)` FLOAT COMMENT '涨速(%)',
         `主力净流入(万元)` FLOAT COMMENT '主力净流入(万元)',
         `成交额(万元)` FLOAT COMMENT '成交额(万元)',
         `5日涨跌幅(%)` FLOAT COMMENT '5日涨跌幅(%)',
         `20日涨跌幅(%)` FLOAT COMMENT '20日涨跌幅(%)',
         `60日涨跌幅(%)` FLOAT COMMENT '60日涨跌幅(%)',
         `52周涨跌幅(%)` FLOAT COMMENT '52周涨跌幅(%)',
         `年初至今涨跌幅(%)` FLOAT COMMENT '年初至今涨跌幅(%)',
         `主力流出(万元)` FLOAT COMMENT '主力流出(万元)',
         `主力流入(万元)` FLOAT COMMENT '主力流入(万元)',
         `5日主力净流入(万元)` FLOAT COMMENT '5日主力净流入(万元)',
         `20日主力净流入(万元)` FLOAT COMMENT '20日主力净流入(万元)',
         `总市值(亿元)` FLOAT COMMENT '总市值(亿元)',
         `流通市值(亿元)` FLOAT COMMENT '流通市值(亿元)',
         `lzg_code` VARCHAR(8) COMMENT '领涨股代码',
         `lzg_code_int` INT COMMENT '领涨股代码整数格式',
         `lzg_symbol` VARCHAR(20) COMMENT '领涨股标识',
         `lzg_name` VARCHAR(20) COMMENT '领涨股名称',
         `领涨股涨跌额` FLOAT COMMENT '领涨股涨跌额',
         `领涨股涨跌幅(%)` FLOAT COMMENT '领涨股涨跌幅(%)',
         `领涨股收盘价` FLOAT COMMENT '领涨股收盘价',
         UNIQUE KEY `uniq_date_code` (`date_int`, `code_int`),
         INDEX `idx_date` (`date`),
         INDEX `idx_code` (`code_int`)
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='腾讯实时行业申万数据表';
     """


# 实时股票
    DBManager.execute_sql(realtime_stock_tx_sql)
    DBManager.execute_sql(realtime_stock_sina_sql)
    DBManager.execute_sql(realtime_stock_df_sql)
# 实时指数
    DBManager.execute_sql(realtime_index_sina_sql)
    DBManager.execute_sql(realtime_index_df_sql)
# 实时基金
    DBManager.execute_sql(realtime_etf_sina_sql)
    DBManager.execute_sql(realtime_etf_df_sql)
# 实时行业
    DBManager.execute_sql(realtime_industry_tx_sql)

# 实时行业
    DBManager.execute_sql(realtime_industry_tx_sql)


if __name__ == "__main__":
    create_tables()