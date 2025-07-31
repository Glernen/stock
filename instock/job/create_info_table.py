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
    # 股票基础信息表
    basic_info_stock_sql = """
    CREATE TABLE IF NOT EXISTS `basic_info_stock` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '股票代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '股票名称',
        `symbol` VARCHAR(20) COMMENT '股票标识',
        `股票类型` VARCHAR(20) COMMENT '股票类型',
        `市场标识` INT COMMENT '市场标识',
        `市场代码` VARCHAR(20) COMMENT '市场代码',
        `东方财富网行业` VARCHAR(60) COMMENT '东方财富网行业',
        `申万1级名` VARCHAR(60) COMMENT '申万1级名',
        `申万1级代码` FLOAT COMMENT '申万1级代码',
        `申万1级数字代码` FLOAT COMMENT '申万1级数字代码',
        `申万2级名` VARCHAR(60) COMMENT '申万2级名',
        `申万2级代码` FLOAT COMMENT '申万2级代码',
        `申万2级数字代码` FLOAT COMMENT '申万2级数字代码',
        `参与指标计算` FLOAT COMMENT '参与指标计算',
        UNIQUE KEY `uniq_code_int` (`code`, `code_int`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基础信息表';
    """

    # 指数基础信息表
    basic_info_index_sql = """
    CREATE TABLE IF NOT EXISTS `basic_info_index` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `code` VARCHAR(6) NOT NULL COMMENT '指数代码',
        `code_int` INT NOT NULL COMMENT '指数代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '指数名称',
        `symbol` VARCHAR(20) COMMENT '指数标识',
        `市场标识` INT COMMENT '市场标识',
        `市场代码` VARCHAR(20) COMMENT '市场代码',
        `参与指标计算` FLOAT COMMENT '参与指标计算',
        UNIQUE KEY `uniq_code_int` (`code`, `code_int`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数基础信息表';
    """

    # 基金基础信息表
    basic_info_etf_sql = """
    CREATE TABLE IF NOT EXISTS `basic_info_etf` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `code` VARCHAR(6) NOT NULL COMMENT '基金代码',
        `code_int` INT NOT NULL COMMENT '基金代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '基金名称',
        `symbol` VARCHAR(20) COMMENT '基金标识',
        `市场标识` INT COMMENT '市场标识',
        `市场代码` VARCHAR(20) COMMENT '市场代码',
        `参与指标计算` FLOAT COMMENT '参与指标计算',
        UNIQUE KEY `uniq_code_int` (`code`, `code_int`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基金基础信息表';
    """

    # 行业基础信息表
    basic_info_industry_sql = """
    CREATE TABLE IF NOT EXISTS `basic_info_industry` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
        `code` VARCHAR(6) NOT NULL COMMENT '行业代码',
        `code_int` INT NOT NULL COMMENT '行业代码整数格式',
        `name` VARCHAR(20) NOT NULL COMMENT '行业名称',
        `symbol` VARCHAR(20) COMMENT '行业标识',
        `市场标识` INT COMMENT '市场标识',
        `市场代码` VARCHAR(20) COMMENT '市场代码',
        `参与指标计算` FLOAT COMMENT '参与指标计算',
        UNIQUE KEY `uniq_code_int` (`code`, `code_int`),
        INDEX `idx_code` (`code_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行业基础信息表';
    """


# 股票基础信息表
    DBManager.execute_sql(basic_info_stock_sql)
# 指数基础信息表
    DBManager.execute_sql(basic_info_index_sql)
# 基金基础信息表
    DBManager.execute_sql(basic_info_etf_sql)
# 行业基础信息表
    DBManager.execute_sql(basic_info_industry_sql)

if __name__ == "__main__":
    create_tables()