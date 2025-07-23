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




def get_market_sentiment_data():
    """核心分析逻辑"""
    try:
        # 确保表存在
        create_optimized_table()

        sql = f"""
        WITH `date_range` AS (
          SELECT DISTINCT
            `index_3day_indicators`.`date_int` AS `date_int`
          FROM
            `index_3day_indicators`
          WHERE
            (
              str_to_date(`index_3day_indicators`.`date_int`, '%Y%m%d') >= ((SELECT max(str_to_date(`index_3day_indicators`.`date_int`, '%Y%m%d')) FROM `index_3day_indicators`) - INTERVAL 2 MONTH)
            )
        ),
        `up_sentiment` AS (
          SELECT
            `index_3day_indicators`.`date_int` AS `date_int`,
            round(((count(DISTINCT `index_3day_indicators`.`code_int`) * 100.0) / 10), 0) AS `指数上涨情绪`
          FROM
            `index_3day_indicators`
          WHERE
            (
              (`index_3day_indicators`.`code_int` IN (1, 9, 11, 300, 510, 399001, 399006, 399293, 852,399106))
              AND (`index_3day_indicators`.`kdjk` >= `index_3day_indicators`.`kdjk_day1`)
              AND (`index_3day_indicators`.`kdjd` >= `index_3day_indicators`.`kdjd_day1`)
              AND (`index_3day_indicators`.`wr_6` >= `index_3day_indicators`.`wr_6_day1`)
              AND (`index_3day_indicators`.`cci` >= `index_3day_indicators`.`cci_day1`)
              AND (`index_3day_indicators`.`kdjk` <= 80)
              AND (`index_3day_indicators`.`kdjd` <= 80)
              AND (`index_3day_indicators`.`kdjj` <= 100)
            )
          GROUP BY
            `index_3day_indicators`.`date_int`
        ),
        `down_sentiment` AS (
          SELECT
            `index_3day_indicators`.`date_int` AS `date_int`,
            round(((count(DISTINCT `index_3day_indicators`.`code_int`) * 100.0) / 10), 0) AS `指数下跌情绪`
          FROM
            `index_3day_indicators`
          WHERE
            (
              (`index_3day_indicators`.`code_int` IN (1, 9, 11, 300, 510, 399001, 399006, 399293, 852,399106))
              AND (`index_3day_indicators`.`kdjk` <= `index_3day_indicators`.`kdjk_day1`)
              AND (`index_3day_indicators`.`kdjd` <= `index_3day_indicators`.`kdjd_day1`)
              AND (`index_3day_indicators`.`wr_6` <= `index_3day_indicators`.`wr_6_day1`)
              AND (`index_3day_indicators`.`cci` <= `index_3day_indicators`.`cci_day1`)
              AND (`index_3day_indicators`.`cci_day1` <= `index_3day_indicators`.`cci_day2`)
              AND (`index_3day_indicators`.`kdjk` >= 50)
              AND (`index_3day_indicators`.`kdjd` >= 50)
            )
          GROUP BY
            `index_3day_indicators`.`date_int`
        ),
        `signals` AS (
          SELECT
            `index_3day_indicators`.`date_int` AS `date_int`,
            sum(
              (
                CASE
                  WHEN (
                      (`index_3day_indicators`.`kdjk` > `index_3day_indicators`.`kdjd`)
                      AND (`index_3day_indicators`.`kdjk_day1` <= `index_3day_indicators`.`kdjd_day1`)
                      AND (`index_3day_indicators`.`kdjd` > `index_3day_indicators`.`kdjd_day1`)
                      AND (`index_3day_indicators`.`cci` > `index_3day_indicators`.`cci_day1`)
                    ) THEN
                    1
                  ELSE
                    0
                END
              )
            ) AS `strong_buy_signal`,
            sum(
              (
                CASE
                  WHEN (
                      (`index_3day_indicators`.`kdjk` < `index_3day_indicators`.`kdjd`)
                      AND (`index_3day_indicators`.`kdjd` < `index_3day_indicators`.`kdjd_day1`)
                      AND (`index_3day_indicators`.`kdjk_day1` >= `index_3day_indicators`.`kdjd_day1`)
                      AND (`index_3day_indicators`.`cci` < `index_3day_indicators`.`cci_day1`)
                    ) THEN
                    1
                  ELSE
                    0
                END
              )
            ) AS `strong_sell_signal`
          FROM
            `index_3day_indicators`
          WHERE
            (`index_3day_indicators`.`code_int` IN (1, 9, 11, 300, 510, 399001, 399006, 399293, 852,399106))
          GROUP BY
            `index_3day_indicators`.`date_int`
        ),
        `final_result` AS (
          SELECT
            `dr`.`date_int` AS `date_int`,
            COALESCE(`us`.`指数上涨情绪`, 0) AS `指数上涨情绪`,
            COALESCE(`ds`.`指数下跌情绪`, 0) AS `指数下跌情绪`,
            COALESCE(`s`.`strong_buy_signal`, 0) AS `strong_buy_signal`,
            COALESCE(`s`.`strong_sell_signal`, 0) AS `strong_sell_signal`,
            (
              CASE
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) BETWEEN 70 AND 100)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) > 5)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '满仓持有：市场极度乐观'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) BETWEEN 70 AND 100)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) <= 5)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '重仓持有：持仓待涨，注意止盈'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) BETWEEN 40 AND 60)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) > 2)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '上行趋势：建议持仓待涨，金叉'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) BETWEEN 40 AND 60)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) <= 2)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '上行趋势：建议分批建仓'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) BETWEEN 20 AND 30)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) > 2)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '上行震荡：建议逢低布局，金叉'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) BETWEEN 20 AND 30)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) <= 2)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '上行震荡：建议高抛低吸'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) BETWEEN 70 AND 100)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) >= 5)
                  ) THEN
                  '一键清仓：市场抛售，等待企稳'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) BETWEEN 70 AND 100)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) < 5)
                  ) THEN
                  '空仓观望：保持现金仓位'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) BETWEEN 40 AND 60)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) > 2)
                  ) THEN
                  '下行趋势：建议反弹减仓，死叉'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) BETWEEN 40 AND 60)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) <= 2)
                  ) THEN
                  '下行趋势：建议控制仓位'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) BETWEEN 20 AND 30)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) > 2)
                  ) THEN
                  '下行震荡：建议反弹减仓，死叉'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) BETWEEN 20 AND 30)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) <= 2)
                  ) THEN
                  '下行震荡：建议控制仓位'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '多空平衡：建议保持观望'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) > 1)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) = 0)
                  ) THEN
                  '多空平衡：可轻仓试水，金叉'
                WHEN (
                    (COALESCE(`us`.`指数上涨情绪`, 0) = 0)
                    AND (COALESCE(`ds`.`指数下跌情绪`, 0) = 0)
                    AND (COALESCE(`s`.`strong_buy_signal`, 0) = 0)
                    AND (COALESCE(`s`.`strong_sell_signal`, 0) > 0)
                  ) THEN
                  '多空平衡：不建议建仓，死叉'
                WHEN ((COALESCE(`us`.`指数上涨情绪`, 0) = 10) OR (COALESCE(`ds`.`指数下跌情绪`, 0) = 10)) THEN
                  '观察阶段：趋势可能反转'
                ELSE
                  '未知状态：建议止盈'
              END
            ) AS `操作建议`
          FROM
            (
              (
                (`date_range` `dr` LEFT JOIN `up_sentiment` `us` ON ((`dr`.`date_int` = `us`.`date_int`)))
                LEFT JOIN `down_sentiment` `ds` ON ((`dr`.`date_int` = `ds`.`date_int`))
              )
              LEFT JOIN `signals` `s` ON ((`dr`.`date_int` = `s`.`date_int`))
            )
        ) SELECT
          `final_result`.`date_int` AS `date_int`,
          `final_result`.`指数上涨情绪` AS `指数上涨情绪`,
          `final_result`.`指数下跌情绪` AS `指数下跌情绪`,
          `final_result`.`strong_buy_signal` AS `strong_buy_signal`,
          `final_result`.`strong_sell_signal` AS `strong_sell_signal`,
          `final_result`.`操作建议` AS `操作建议`,
          (
            CASE
              WHEN (`final_result`.`操作建议` IS NOT NULL) THEN
                LEFT(REPLACE(REPLACE(`final_result`.`操作建议`, '：', ''), '（', ''), 4)
              ELSE
                '未知情绪'
            END
          ) AS `市场情绪`
        FROM
          `final_result`
        ORDER BY
          `final_result`.`date_int`
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






def create_optimized_table():
    table_name = "market_sentiment_a"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `date_int` INT NOT NULL COMMENT '日期YYYYMMDD格式',
        `指数上涨情绪` FLOAT,
        `指数下跌情绪` FLOAT,
        `strong_buy_signal` INT,
        `strong_sell_signal` INT,
        `操作建议` VARCHAR(120),
        `市场情绪` VARCHAR(50),
        UNIQUE KEY `idx_date` (`date_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='市场情绪分析表';
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
    table_name = "market_sentiment_a"
    try:
        with mdb.engine().connect() as conn:
            # 确保表结构存在且最新
            create_optimized_table()
            
            # 定义唯一键和需要更新的字段
            unique_keys = {'date_int'}
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=conn.engine)
            
            # 排除唯一键和自增主键，其他字段均更新
            update_columns = [col.name for col in table.columns 
                             if col.name not in unique_keys and col.name != 'id']

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
        print("数据插入/更新成功")
        logging.info("数据插入/更新成功")
        
    except Exception as e:
        logging.error(f"操作失败，详细错误：{str(e)}", exc_info=True)
        raise


# 优化后的数据插入函数
# def optimized_data_insert(data):
#     table_name = "market_sentiment_a"
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