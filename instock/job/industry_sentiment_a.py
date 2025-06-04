#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, text
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 
import instock.lib.database as mdb
import instock.core.tablestructure as tbs

__author__ = 'hqm'
__date__ = '2025年4月20日'

'''
主要逻辑：访问数据库，将获取的数据保存到industry_sentiment_a数据表中
1、sql获取数据：
sql：        WITH 行业基础数据 AS (
            SELECT 
                name AS 行业名称,
                date_int AS 日期,
                -- 原始指标
                kdjK, kdjD, kdjk_day1, kdjd_day1, kdjk_day2, kdjd_day2,
                wr_6, wr_6_day1, wr_6_day2,
                cci, cci_day1, cci_day2
            FROM industry_3day_indicators
        ),
        趋势判断 AS (
            SELECT 
                行业名称,
                日期,
                -- KDJ趋势判断
                CASE 
                    WHEN kdjK > kdjk_day1 AND kdjD > kdjd_day1 
--                         AND kdjk_day1 <= kdjk_day2 AND kdjd_day1 <= kdjd_day2 
                    THEN '上涨趋势'
                    WHEN kdjK < kdjk_day1 AND kdjD < kdjd_day1 
--                         AND kdjk_day1 >= kdjk_day2 AND kdjd_day1 >= kdjd_day2 
                    THEN '下降趋势'
                    ELSE '趋势中性'
                END AS KDJ趋势,
                
                -- 威廉指标趋势
                CASE 
                    WHEN wr_6 > wr_6_day1 AND wr_6_day1 > wr_6_day2 
                    THEN '持续超卖'
                    WHEN wr_6 < wr_6_day1 AND wr_6_day1 < wr_6_day2 
                    THEN '持续超买'
                    ELSE '震荡状态'
                END AS WR趋势,
                
                -- CCI趋势
                CASE 
                    WHEN cci > cci_day1 AND cci_day1 > cci_day2 
                    THEN '多头强化'
                    WHEN cci < cci_day1 AND cci_day1 < cci_day2 
                    THEN '空头强化'
                    ELSE '方向不明'
                END AS CCI趋势
            FROM 行业基础数据
        ),
        买卖信号 AS (
            SELECT 
                趋势判断.行业名称,
                趋势判断.日期,
                趋势判断.KDJ趋势,
                趋势判断.WR趋势,
                趋势判断.CCI趋势,
                -- 强烈买入信号（行业级复合条件）
                CASE 
                    WHEN 趋势判断.KDJ趋势 = '上涨趋势' 
--                 AND 趋势判断.WR趋势 = '持续超卖' 
                        AND 趋势判断.CCI趋势 = '多头强化' 
                        AND 行业基础数据.cci > -150  -- 当前值突破阈值
                    THEN 1 
                    ELSE 0 
                END AS 强烈买入信号,
                
                -- 强烈卖出信号（行业级复合条件）
                CASE 
                    WHEN 趋势判断.KDJ趋势 = '下降趋势' 
--                 AND 趋势判断.WR趋势 = '持续超买' 
                        AND 趋势判断.CCI趋势 = '空头强化' 
                        AND 行业基础数据.cci < 150  -- 当前值跌破阈值
                    THEN 1 
                    ELSE 0 
                END AS 强烈卖出信号
            FROM 趋势判断
            JOIN 行业基础数据 USING (行业名称, 日期)
        )
        SELECT 
            日期,
            行业名称,
            KDJ趋势,
            WR趋势,
            CCI趋势,
            强烈买入信号,
            强烈卖出信号,
            -- 综合情绪判断
            CASE 
                WHEN 强烈买入信号 = 1 AND 强烈卖出信号 = 0 THEN '行业强势看涨'
                WHEN 强烈卖出信号 = 1 AND 强烈买入信号 = 0 THEN '行业弱势看跌'
                WHEN 强烈买入信号 + 强烈卖出信号 = 0 THEN '行业震荡中性'
                ELSE '多空博弈激烈' 
            END AS 行业情绪
        FROM 买卖信号
--         WHERE KDJ趋势 = '上涨趋势'

-- 获取的字段为date_int,指数上涨情绪，指数下跌情绪，操作建议，市场情绪

2、industry_sentiment_a表的字段：id(自生成主键)，date_int（int类型），`行业名称` VARCHAR(50), `KDJ趋势` VARCHAR(50), `WR趋势` VARCHAR(50), `CCI趋势` VARCHAR(50), `强烈买入信号` FLOAT, `强烈卖入信号` FLOAT, `行业情绪` VARCHAR(50),

'''



def get_market_sentiment_data():
    """核心分析逻辑"""
    try:
        # 确保表存在
        create_optimized_table()

        sql = f"""
        WITH 行业基础数据 AS (
            SELECT 
                name AS 行业名称,
                date_int AS date_int,
                -- 原始指标
                kdjK, kdjD, kdjk_day1, kdjd_day1, kdjk_day2, kdjd_day2,
                wr_6, wr_6_day1, wr_6_day2,
                cci, cci_day1, cci_day2
            FROM industry_3day_indicators
          WHERE
            (
              str_to_date(`industry_3day_indicators`.`date_int`, '%Y%m%d') >= ((SELECT max(str_to_date(`industry_3day_indicators`.`date_int`, '%Y%m%d')) FROM `industry_3day_indicators`) - INTERVAL 1 MONTH)
            )
        ),
        趋势判断 AS (
            SELECT 
                行业名称,
                date_int,
                -- KDJ趋势判断
                CASE 
                    WHEN kdjK > kdjk_day1 AND kdjD > kdjd_day1 
--                         AND kdjk_day1 <= kdjk_day2 AND kdjd_day1 <= kdjd_day2 
                    THEN '上涨趋势'
                    WHEN kdjK < kdjk_day1 AND kdjD < kdjd_day1 
--                         AND kdjk_day1 >= kdjk_day2 AND kdjd_day1 >= kdjd_day2 
                    THEN '下降趋势'
                    ELSE '趋势中性'
                END AS KDJ趋势,
                
                -- 威廉指标趋势
                CASE 
                    WHEN wr_6 > wr_6_day1 AND wr_6_day1 > wr_6_day2 
                    THEN '持续超卖'
                    WHEN wr_6 < wr_6_day1 AND wr_6_day1 < wr_6_day2 
                    THEN '持续超买'
                    ELSE '震荡状态'
                END AS WR趋势,
                
                -- CCI趋势
                CASE 
                    WHEN cci > cci_day1 AND cci_day1 > cci_day2 
                    THEN '多头强化'
                    WHEN cci < cci_day1 AND cci_day1 < cci_day2 
                    THEN '空头强化'
                    ELSE '方向不明'
                END AS CCI趋势
            FROM 行业基础数据
        ),
        买卖信号 AS (
            SELECT 
                趋势判断.行业名称,
                趋势判断.date_int,
                趋势判断.KDJ趋势,
                趋势判断.WR趋势,
                趋势判断.CCI趋势,
                -- 强烈买入信号（行业级复合条件）
                CASE 
                    WHEN 趋势判断.KDJ趋势 = '上涨趋势' 
--                 AND 趋势判断.WR趋势 = '持续超卖' 
                        AND 趋势判断.CCI趋势 = '多头强化' 
                        AND 行业基础数据.cci > -150  -- 当前值突破阈值
                    THEN 1 
                    ELSE 0 
                END AS 强烈买入信号,
                
                -- 强烈卖出信号（行业级复合条件）
                CASE 
                    WHEN 趋势判断.KDJ趋势 = '下降趋势' 
--                 AND 趋势判断.WR趋势 = '持续超买' 
                        AND 趋势判断.CCI趋势 = '空头强化' 
                        AND 行业基础数据.cci < 150  -- 当前值跌破阈值
                    THEN 1 
                    ELSE 0 
                END AS 强烈卖出信号
            FROM 趋势判断
            JOIN 行业基础数据 USING (行业名称, date_int)
        )
        SELECT 
            date_int,
            行业名称,
            KDJ趋势,
            WR趋势,
            CCI趋势,
            强烈买入信号,
            强烈卖出信号,
            -- 综合情绪判断
            CASE 
                WHEN 强烈买入信号 = 1 AND 强烈卖出信号 = 0 THEN '行业强势看涨'
                WHEN 强烈卖出信号 = 1 AND 强烈买入信号 = 0 THEN '行业弱势看跌'
                WHEN 强烈买入信号 + 强烈卖出信号 = 0 THEN '行业震荡中性'
                ELSE '多空博弈激烈' 
            END AS 行业情绪
        FROM 买卖信号
--         WHERE KDJ趋势 = '上涨趋势'
        """
        
        with mdb.engine().connect() as conn:
            data = pd.read_sql(text(sql), conn)
        
        if data.empty:
            logging.info("无符合条件的数据")
            return


        print(f'{data}')

        # 插入数据库逻辑（使用优化后的插入方法）
        optimized_data_insert(data)
        
    except Exception as e:
        logging.error(f"处理异常：{e}")
        raise




# 修改表结构（新增字段）
def create_optimized_table():
    table_name = "industry_sentiment_a"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `date_int` INT NOT NULL COMMENT '日期YYYYMMDD格式',
        `行业名称` VARCHAR(50),
        `KDJ趋势` VARCHAR(50),
        `WR趋势` VARCHAR(50),
        `CCI趋势` VARCHAR(50),
        `强烈买入信号` INT,
        `强烈卖出信号` INT,
        `行业情绪` VARCHAR(50),
        UNIQUE KEY `idx_date_名称` (`date_int`,`行业名称`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='行业情绪分析表';
    """
    
    # 使用新的连接检查并创建表
    with mdb.engine().connect() as conn:
        try:
            # 检查表是否存在
            if not conn.dialect.has_table(conn, table_name):
                conn.execute(text(create_table_sql))
                conn.commit()
                logging.info(f"表 {table_name} 创建成功")
            else:
                # 检查并添加缺少的列（可选）
                pass
        except Exception as e:
            logging.error(f"创建表时发生错误: {e}")
            raise



def optimized_data_insert(data):
    table_name = "industry_sentiment_a"
    try:
        with mdb.engine().connect() as conn:
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=conn.engine)
            
            # 调试：打印表结构
            # print("\n[DEBUG] 表结构字段：")
            # for col in table.columns:
            #     print(f"{col.name}: {col.type}")
                
            unique_keys = {'date_int', '行业名称'}
            update_columns = [
                col.name for col in table.columns 
                if col.name not in unique_keys 
                and col.name != 'id'
            ]
            # print(f"\n[DEBUG] 将更新的字段：{update_columns}")

            # 验证数据字段匹配
            missing_columns = set(data.columns) - {col.name for col in table.columns}
            if missing_columns:
                raise ValueError(f"数据包含表中不存在的列: {missing_columns}")

        def upsert_data(table, conn, keys, data_iter):
            from sqlalchemy.dialects.mysql import insert
            
            data_rows = [dict(zip(keys, row)) for row in data_iter]
            if not data_rows:
                return
                
            stmt = insert(table.table).values(data_rows)
            update_dict = {col: stmt.inserted[col] for col in update_columns}
            
            # 调试：打印生成的SQL
            compiled_stmt = stmt.on_duplicate_key_update(**update_dict).compile(
                compile_kwargs={"literal_binds": True}
            )
            # print(f"\n[DEBUG] 执行SQL:\n{compiled_stmt}")
            
            result = conn.execute(stmt.on_duplicate_key_update(**update_dict))
            # print(f"[DEBUG] 影响行数: {result.rowcount}")

        data.to_sql(
            name=table_name,
            con=mdb.engine(),
            if_exists='append',
            index=False,
            chunksize=500,  # 减小chunksize便于调试
            method=upsert_data
        )
        # print("数据插入/更新成功")
        logging.info("数据插入/更新成功")
        
    except Exception as e:
        logging.error(f"操作失败，详细错误：{str(e)}", exc_info=True)
        raise


# 优化后的数据插入函数
# def optimized_data_insert(data):
#     table_name = "industry_sentiment_a"
#     try:
#         # 自定义插入方法，使用INSERT IGNORE避免重复
#         def insert_ignore(table, conn, keys, data_iter):
#             from sqlalchemy.dialects.mysql import insert
#             data_rows = [dict(zip(keys, row)) for row in data_iter]
#             if not data_rows:
#                 return
#             stmt = insert(table.table).values(data_rows).prefix_with('IGNORE')
#             conn.execute(stmt)
        
#         # 使用自定义方法插入数据
#         data.to_sql(
#             name=table_name,
#             con=mdb.engine(),
#             if_exists='append',
#             index=False,
#             chunksize=500,
#             method=insert_ignore
#         )
#         logging.info("数据插入完成，重复记录已自动忽略")
#     except Exception as e:
#         logging.error(f"数据插入失败: {e}")
#         raise



def main():
    """主入口函数"""
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    try:
        get_market_sentiment_data()
    except Exception as e:
        logging.error(f"程序运行异常: {e}")

if __name__ == '__main__':
    main()